import os
import asyncio
import logging
from datetime import datetime, timedelta
import pytz
import re
import pandas as pd

# Third-party libraries
from sqlalchemy import Column, Integer, String, Float, DateTime, select, update, func, label
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.sql import Select
from dotenv import load_dotenv

# Import from python-telegram-bot
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
    ConversationHandler,
    CallbackQueryHandler
)

# --- Configuration and Logging Setup ---
load_dotenv()  # Load environment variables from .env file

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Environment Variables
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x]
TIMEZONE = os.getenv("TIMEZONE", "Africa/Cairo") # Changed to a common TZ for better default awareness
EXPORT_DIR = os.getenv("EXPORT_DIR", "/app/exports")
RATE_LIMIT_PER_MINUTE = int(os.getenv("RATE_LIMIT_PER_MINUTE", 10))
# DEFAULT_CURRENCY removed as it will be dynamic
MIN_WITHDRAWAL_AMOUNT = float(os.getenv("MIN_WITHDRAWAL_AMOUNT", 50.0))
DATABASE_URL = os.getenv("DATABASE_URL")

# Exchange rates to USD (as of 2025-09-04)
SAR_TO_USD = 0.2665
AED_TO_USD = 0.2723

# Validate essential environment variables
if not BOT_TOKEN:
    logger.critical("BOT_TOKEN is not set. Please set it in the .env file.")
    exit(1)
if not DATABASE_URL:
    logger.critical("DATABASE_URL is not set. Please set it in the .env file.")
    exit(1)

# Ensure EXPORT_DIR exists
os.makedirs(EXPORT_DIR, exist_ok=True)

# --- Database Setup ---
Base = declarative_base()

class Affiliate(Base):
    __tablename__ = "affiliates"
    id = Column(Integer, primary_key=True)
    telegram_id = Column(Integer, unique=True, nullable=False)
    name = Column(String, nullable=False)
    phone = Column(String, nullable=False) # Egyptian phone
    store_name = Column(String, nullable=False)
    balance = Column(Float, default=0.0, nullable=False)  # Balance in USD
    total_earnings = Column(Float, default=0.0, nullable=False)  # Total commissions earned in USD
    total_sales = Column(Float, default=0.0, nullable=False)  # Total sales in USD
    total_orders = Column(Integer, default=0, nullable=False)

    def __repr__(self):
        return f"<Affiliate(id={self.id}, name='{self.name}', telegram_id={self.telegram_id})>"

class Order(Base):
    __tablename__ = "orders"
    id = Column(Integer, primary_key=True)
    affiliate_id = Column(Integer, nullable=False)
    customer_name = Column(String, nullable=False)
    customer_phone = Column(String, nullable=False)
    address = Column(String, nullable=False)  # New field for detailed address
    city = Column(String, nullable=False)
    country = Column(String, nullable=False) # Added country
    currency = Column(String, nullable=False) # Added currency
    product = Column(String, nullable=False)
    product_code = Column(String, nullable=False)  # Made required
    cost_price = Column(Float, nullable=False)  # New field for original cost price
    selling_price = Column(Float, nullable=False)  # New field for selling price
    commission = Column(Float, nullable=False)
    status = Column(String, default="pending", nullable=False)  # pending, delivered, issue
    created_at = Column(DateTime(timezone=True), nullable=False) # Made timezone aware

    def __repr__(self):
        return f"<Order(id={self.id}, affiliate_id={self.affiliate_id}, selling_price={self.selling_price}, country={self.country})>"

class Withdrawal(Base):
    __tablename__ = "withdrawals"
    id = Column(Integer, primary_key=True)
    affiliate_id = Column(Integer, nullable=False)
    amount = Column(Float, nullable=False)
    phone = Column(String, nullable=False)
    status = Column(String, default="pending", nullable=False) # pending, approved, rejected
    currency = Column(String, nullable=False) # Added currency for withdrawal
    requested_at = Column(DateTime(timezone=True), nullable=False) # Made timezone aware
    processed_at = Column(DateTime(timezone=True), nullable=True)
    processed_by_admin_id = Column(Integer, nullable=True)

    def __repr__(self):
        return f"<Withdrawal(id={self.id}, affiliate_id={self.affiliate_id}, amount={self.amount}, status={self.status})>"

try:
    engine = create_async_engine(DATABASE_URL, echo=False)
    SessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
except Exception as e:
    logger.critical(f"Failed to create database engine: {e}")
    exit(1)

async def init_db():
    try:
        async with engine.begin() as conn:
            # Drop all tables and recreate them to ensure schema updates are applied.
            # In a production environment, you would use Alembic for migrations.
            # For this example, it's simpler to recreate.
            logger.warning("Dropping existing tables and recreating them. ALL DATA WILL BE LOST!")
            await conn.run_sync(Base.metadata.drop_all)
            await conn.run_sync(Base.metadata.create_all)
        logger.info("Database tables (re)created successfully.")
    except Exception as e:
        logger.critical(f"Failed to initialize database tables: {e}")
        raise

# --- Utility Functions ---
def get_now_timezone_aware():
    return datetime.now(pytz.timezone(TIMEZONE))

def validate_affiliate_phone(phone: str) -> bool:
    # Egyptian phone number
    pattern = r"^\+20\d{10}$" # +20 followed by 10 digits
    return bool(re.match(pattern, phone))

def validate_customer_phone(phone: str, country: str) -> bool:
    if country == "Saudi Arabia":
        pattern = r"^\+966\d{9}$" # +966 followed by 9 digits
    elif country == "UAE":
        pattern = r"^\+971\d{9}$" # +971 followed by 9 digits
    else:
        return False
    return bool(re.match(pattern, phone))

def get_currency_for_country(country: str) -> str:
    return "SAR" if country == "Saudi Arabia" else "AED" if country == "UAE" else "N/A"

def convert_to_usd(amount: float, currency: str) -> float:
    if currency == "SAR":
        return amount * SAR_TO_USD
    elif currency == "AED":
        return amount * AED_TO_USD
    elif currency == "USD":
        return amount
    else:
        raise ValueError(f"Unknown currency: {currency}")

# --- Conversation States ---
REGISTER_NAME, REGISTER_PHONE, REGISTER_STORE_NAME = range(3)
ORDER_CUSTOMER_NAME, ORDER_CUSTOMER_PHONE, ORDER_ADDRESS, ORDER_CITY, ORDER_COUNTRY, ORDER_PRODUCT, ORDER_PRODUCT_CODE, ORDER_COST_PRICE, ORDER_SELLING_PRICE = range(3, 12)
WITHDRAWAL_AMOUNT, WITHDRAWAL_PHONE = range(12, 14)
ADMIN_MENU, ADMIN_WITHDRAWALS_MENU, ADMIN_ORDERS_MENU = range(14, 17)

# --- Rate Limiting ---
async def rate_limit_check(affiliate_id: int) -> bool:
    async with SessionLocal() as session:
        now_tz = get_now_timezone_aware()
        one_minute_ago = now_tz - timedelta(minutes=1)
        result = await session.execute(
            select(func.count()).select_from(Order).where(
                Order.affiliate_id == affiliate_id,
                Order.created_at >= one_minute_ago
            )
        )
        count = result.scalar_one()
        logger.info(f"User {affiliate_id} made {count} orders in the last minute. Limit: {RATE_LIMIT_PER_MINUTE}")
        return count < RATE_LIMIT_PER_MINUTE

# --- Keyboard Markups ---
def main_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("🛒 طلب أوردر")],
            [KeyboardButton("📦 طلباتي السابقة")],
            [KeyboardButton("💳 طلب سحب")],
            [KeyboardButton("💰 كشف حساب العمولة")]
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )

def admin_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("📊 إحصاءات المسوّقين")],
            [KeyboardButton("📦 عرض جميع الطلبات")],
            [KeyboardButton("🛠 إدارة الطلبات")],
            [KeyboardButton("💵 إدارة طلبات السحب")],
            [KeyboardButton("📁 تصدير شامل (Excel)")],
            [KeyboardButton("🔙 العودة إلى القائمة الرئيسية")]
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )

def country_selection_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("المملكة العربية السعودية")],
            [KeyboardButton("الإمارات العربية المتحدة")],
            [KeyboardButton("إلغاء")]
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )

# --- Handlers ---
async def start_command(tg_update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = tg_update.effective_user.id
    async with SessionLocal() as session:
        result = await session.execute(select(Affiliate).where(Affiliate.telegram_id == user_id))
        affiliate = result.scalar_one_or_none()
        if affiliate:
            await tg_update.message.reply_text("مرحبًا بك مرة أخرى! اختر من القائمة:", reply_markup=main_menu())
            return ConversationHandler.END
        else:
            await tg_update.message.reply_text("مرحبًا! يرجى التسجيل أولاً. أدخل اسمك الكامل:")
            return REGISTER_NAME

async def register_name(tg_update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    name = tg_update.message.text.strip()
    if not name or len(name) < 2:
        await tg_update.message.reply_text("الاسم غير صالح. يرجى إدخال اسم حقيقي.")
        return REGISTER_NAME
    context.user_data['registration_name'] = name
    await tg_update.message.reply_text("أدخل رقم هاتفك المصري (مثال: +201234567890):")
    return REGISTER_PHONE

async def register_phone(tg_update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    phone = tg_update.message.text.strip()
    if not validate_affiliate_phone(phone):
        await tg_update.message.reply_text("رقم الهاتف غير صالح. يرجى إدخال رقم مصري صحيح يبدأ بـ +20 و10 أرقام بعده.")
        return REGISTER_PHONE
    context.user_data['registration_phone'] = phone
    await tg_update.message.reply_text("أدخل اسم متجرك أو عملك:")
    return REGISTER_STORE_NAME

async def register_store_name(tg_update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    store_name = tg_update.message.text.strip()
    if not store_name or len(store_name) < 2:
        await tg_update.message.reply_text("اسم المتجر غير صالح. يرجى إدخال اسم حقيقي.")
        return REGISTER_STORE_NAME
    user_id = tg_update.effective_user.id
    name = context.user_data.get('registration_name')
    phone = context.user_data.get('registration_phone')
    async with SessionLocal() as session:
        try:
            affiliate = Affiliate(telegram_id=user_id, name=name, phone=phone, store_name=store_name)
            session.add(affiliate)
            await session.commit()
            await tg_update.message.reply_text("تم التسجيل بنجاح! اختر من القائمة:", reply_markup=main_menu())
            logger.info(f"New affiliate registered: {name} (ID: {user_id})")
        except Exception as e:
            await session.rollback()
            logger.error(f"Error during affiliate registration for {user_id}: {e}", exc_info=True)
            await tg_update.message.reply_text("حدث خطأ أثناء التسجيل. يرجى المحاولة مرة أخرى.")
        finally:
            context.user_data.clear()
            return ConversationHandler.END

async def cancel_conversation(tg_update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await tg_update.message.reply_text("تم إلغاء العملية.", reply_markup=main_menu())
    context.user_data.clear()
    return ConversationHandler.END

async def start_order(tg_update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = tg_update.effective_user.id
    async with SessionLocal() as session:
        result = await session.execute(select(Affiliate).where(Affiliate.telegram_id == user_id))
        affiliate = result.scalar_one_or_none()
        if not affiliate:
            await tg_update.message.reply_text("يرجى التسجيل أولاً باستخدام /start", reply_markup=main_menu())
            return ConversationHandler.END
        if not await rate_limit_check(affiliate.id):
            await tg_update.message.reply_text("لقد تجاوزت الحد الأقصى للطلبات في الدقيقة. يرجى الانتظار قليلاً والمحاولة لاحقًا.", reply_markup=main_menu())
            return ConversationHandler.END
        context.user_data['affiliate_id'] = affiliate.id
        await tg_update.message.reply_text("أدخل اسم العميل كاملاً:")
        return ORDER_CUSTOMER_NAME

async def order_customer_name(tg_update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    customer_name = tg_update.message.text.strip()
    if not customer_name or len(customer_name) < 2:
        await tg_update.message.reply_text("اسم العميل غير صالح. يرجى إدخال اسم حقيقي.")
        return ORDER_CUSTOMER_NAME
    context.user_data['order_customer_name'] = customer_name
    await tg_update.message.reply_text("اختر بلد العميل:", reply_markup=country_selection_keyboard())
    return ORDER_COUNTRY

async def order_country(tg_update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    country = tg_update.message.text.strip()
    if country == "المملكة العربية السعودية":
        context.user_data['order_country'] = "Saudi Arabia"
    elif country == "الإمارات العربية المتحدة":
        context.user_data['order_country'] = "UAE"
    else:
        await tg_update.message.reply_text("اختيار غير صالح. يرجى اختيار 'المملكة العربية السعودية' أو 'الإمارات العربية المتحدة'.")
        return ORDER_COUNTRY

    context.user_data['order_currency'] = get_currency_for_country(context.user_data['order_country'])
    await tg_update.message.reply_text(f"أدخل رقم هاتف العميل لـ {country} (مثال: +966123456789 أو +971123456789):", reply_markup=ReplyKeyboardMarkup([["إلغاء"]], resize_keyboard=True))
    return ORDER_CUSTOMER_PHONE

async def order_customer_phone(tg_update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    customer_phone = tg_update.message.text.strip()
    country = context.user_data.get('order_country')
    if not validate_customer_phone(customer_phone, country):
        await tg_update.message.reply_text(f"رقم الهاتف غير صالح لـ {country}. يرجى إدخال رقم صحيح.")
        return ORDER_CUSTOMER_PHONE
    context.user_data['order_customer_phone'] = customer_phone
    await tg_update.message.reply_text("أدخل عنوان العميل التفصيلي:")
    return ORDER_ADDRESS

async def order_address(tg_update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    address = tg_update.message.text.strip()
    if not address or len(address) < 5:
        await tg_update.message.reply_text("العنوان غير صالح. يرجى إدخال عنوان تفصيلي.")
        return ORDER_ADDRESS
    context.user_data['order_address'] = address
    await tg_update.message.reply_text("أدخل المدينة (مثال: الرياض، دبي، أبوظبي):")
    return ORDER_CITY

async def order_city(tg_update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    city = tg_update.message.text.strip()
    if not city or len(city) < 2:
        await tg_update.message.reply_text("اسم المدينة غير صالح. يرجى إدخال اسم مدينة حقيقي.")
        return ORDER_CITY
    context.user_data['order_city'] = city
    await tg_update.message.reply_text("أدخل اسم المنتج:")
    return ORDER_PRODUCT

async def order_product(tg_update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    product = tg_update.message.text.strip()
    if not product or len(product) < 2:
        await tg_update.message.reply_text("اسم المنتج غير صالح. يرجى إدخال اسم منتج حقيقي.")
        return ORDER_PRODUCT
    context.user_data['order_product'] = product
    await tg_update.message.reply_text("أدخل كود المنتج (إلزامي):")
    return ORDER_PRODUCT_CODE

async def order_product_code(tg_update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    product_code = tg_update.message.text.strip()
    if not product_code:
        await tg_update.message.reply_text("كود المنتج إلزامي. يرجى إدخال كود صحيح.")
        return ORDER_PRODUCT_CODE
    context.user_data['order_product_code'] = product_code
    currency = context.user_data.get('order_currency', 'SAR')
    await tg_update.message.reply_text(f"أدخل سعر المنتج الأصلي (بـ {currency}):")
    return ORDER_COST_PRICE

async def order_cost_price(tg_update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    currency = context.user_data.get('order_currency', 'SAR')
    try:
        cost_price = float(tg_update.message.text.strip())
        if cost_price <= 0:
            await tg_update.message.reply_text("سعر المنتج الأصلي يجب أن يكون أكبر من 0. يرجى إدخال سعر صحيح.")
            return ORDER_COST_PRICE
    except ValueError:
        await tg_update.message.reply_text("يرجى إدخال سعر صحيح (رقم).")
        return ORDER_COST_PRICE
    context.user_data['order_cost_price'] = cost_price
    await tg_update.message.reply_text(f"أدخل سعر البيع (بـ {currency}):")
    return ORDER_SELLING_PRICE

async def order_selling_price(tg_update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    currency = context.user_data.get('order_currency', 'SAR')
    try:
        selling_price = float(tg_update.message.text.strip())
        if selling_price <= 0:
            await tg_update.message.reply_text("سعر البيع يجب أن يكون أكبر من 0. يرجى إدخال سعر صحيح.")
            return ORDER_SELLING_PRICE
        cost_price = context.user_data.get('order_cost_price')
        if selling_price <= cost_price:
            await tg_update.message.reply_text("سعر البيع يجب أن يكون أكبر من سعر المنتج الأصلي للحصول على عمولة إيجابية.")
            return ORDER_SELLING_PRICE
    except ValueError:
        await tg_update.message.reply_text("يرجى إدخال سعر صحيح (رقم).")
        return ORDER_SELLING_PRICE
    
    affiliate_id = context.user_data.get('affiliate_id')
    customer_name = context.user_data.get('order_customer_name')
    customer_phone = context.user_data.get('order_customer_phone')
    address = context.user_data.get('order_address')
    city = context.user_data.get('order_city')
    country = context.user_data.get('order_country')
    product = context.user_data.get('order_product')
    product_code = context.user_data.get('order_product_code')
    
    commission = selling_price - context.user_data['order_cost_price']
    async with SessionLocal() as session:
        try:
            result = await session.execute(select(Affiliate).where(Affiliate.id == affiliate_id))
            affiliate = result.scalar_one_or_none()
            if not affiliate:
                await tg_update.message.reply_text("حدث خطأ: لم يتم العثور على حساب المسوّق الخاص بك. يرجى المحاولة مرة أخرى.", reply_markup=main_menu())
                context.user_data.clear()
                return ConversationHandler.END

            order = Order(
                affiliate_id=affiliate.id,
                customer_name=customer_name,
                customer_phone=customer_phone,
                address=address,
                city=city,
                country=country,
                currency=currency,
                product=product,
                product_code=product_code,
                cost_price=context.user_data['order_cost_price'],
                selling_price=selling_price,
                commission=commission,
                created_at=get_now_timezone_aware()
            )
            session.add(order)
            await session.execute(
                update(Affiliate)
                .where(Affiliate.id == affiliate.id)
                .values(
                    total_orders=Affiliate.total_orders + 1
                )
            )
            await session.commit()
            await tg_update.message.reply_text(f"تم تسجيل الطلب بنجاح! العمولة المحتملة: {convert_to_usd(commission, currency):.2f} USD (سيتم إضافتها بعد التأكيد)", reply_markup=main_menu())
            logger.info(f"Order created by {affiliate.name} (ID: {affiliate.id}). Order ID: {order.id}")
        except Exception as e:
            await session.rollback()
            logger.error(f"Error creating order for {tg_update.effective_user.id}: {e}", exc_info=True)
            await tg_update.message.reply_text("حدث خطأ أثناء تسجيل الطلب. يرجى المحاولة مرة أخرى.", reply_markup=main_menu())
        finally:
            context.user_data.clear()
            return ConversationHandler.END

async def cmd_my_orders(tg_update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = tg_update.effective_user.id
    async with SessionLocal() as session:
        result = await session.execute(select(Affiliate).where(Affiliate.telegram_id == user_id))
        affiliate = result.scalar_one_or_none()
        if not affiliate:
            await tg_update.message.reply_text("يرجى التسجيل أولاً باستخدام /start")
            return
        result = await session.execute(select(Order).where(Order.affiliate_id == affiliate.id).order_by(Order.created_at.desc()))
        orders = result.scalars().all()
        if not orders:
            await tg_update.message.reply_text("لا توجد طلبات مسجلة حتى الآن.")
            return
        response = f"📦 طلباتك السابقة ({len(orders)}):\n\n"
        for order in orders[:10]: # Displaying last 10 orders
            usd_commission = convert_to_usd(order.commission, order.currency)
            commission_text = f"{usd_commission:.2f} USD (مؤكدة)" if order.status == "delivered" else f"{usd_commission:.2f} USD (غير مؤكدة)"
            status_text = "تم التوصيل" if order.status == "delivered" else "في الانتظار" if order.status == "pending" else "هناك مشكلة - تواصل مع الدعم"
            response += (
                f"🆔 {order.id} | العميل: {order.customer_name} ({order.country})\n"
                f"  العنوان: {order.address}, {order.city}\n"
                f"  المنتج: {order.product} | كود المنتج: {order.product_code}\n"
                f"  سعر الأصلي: {order.cost_price:.2f} {order.currency} | سعر البيع: {order.selling_price:.2f} {order.currency}\n"
                f"  العمولة: {commission_text}\n"
                f"  الحالة: {status_text} | التاريخ: {order.created_at.strftime('%Y-%m-%d %H:%M')}\n\n"
            )
        if len(orders) > 10:
            response += "... والمزيد من الطلبات. يرجى التواصل مع الدعم للحصول على سجل كامل."
        await tg_update.message.reply_text(response)

async def start_withdrawal(tg_update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = tg_update.effective_user.id
    async with SessionLocal() as session:
        result = await session.execute(select(Affiliate).where(Affiliate.telegram_id == user_id))
        affiliate = result.scalar_one_or_none()
        if not affiliate:
            await tg_update.message.reply_text("يرجى التسجيل أولاً باستخدام /start", reply_markup=main_menu())
            return ConversationHandler.END
        
        # Check for pending withdrawals
        pending_withdrawals = await session.execute(
            select(Withdrawal).where(
                Withdrawal.affiliate_id == affiliate.id,
                Withdrawal.status == "pending"
            )
        )
        if pending_withdrawals.scalars().first():
            await tg_update.message.reply_text("لديك بالفعل طلب سحب قيد المراجعة. يرجى الانتظار حتى يتم معالجته قبل طلب سحب جديد.", reply_markup=main_menu())
            return ConversationHandler.END

        if affiliate.balance < MIN_WITHDRAWAL_AMOUNT:
            await tg_update.message.reply_text(
                f"رصيدك الحالي ({affiliate.balance:.2f} USD) أقل من الحد الأدنى للسحب ({MIN_WITHDRAWAL_AMOUNT:.2f} USD).",
                reply_markup=main_menu()
            )
            return ConversationHandler.END
        context.user_data['affiliate_id'] = affiliate.id
        context.user_data['affiliate_balance'] = affiliate.balance
        # For withdrawals, we assume USD
        context.user_data['withdrawal_currency'] = "USD"
        await tg_update.message.reply_text(f"أدخل المبلغ المراد سحبه (بـ USD, الحد الأقصى: {affiliate.balance:.2f}):")
        return WITHDRAWAL_AMOUNT

async def withdrawal_amount(tg_update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    currency = context.user_data.get('withdrawal_currency', 'USD')
    try:
        amount = float(tg_update.message.text.strip())
        if amount <= 0:
            await tg_update.message.reply_text("المبلغ يجب أن يكون أكبر من 0. يرجى إدخال مبلغ صحيح.")
            return WITHDRAWAL_AMOUNT
    except ValueError:
        await tg_update.message.reply_text("يرجى إدخال مبلغ صحيح (رقم).")
        return WITHDRAWAL_AMOUNT
    affiliate_balance = context.user_data.get('affiliate_balance')
    if amount > affiliate_balance:
        await tg_update.message.reply_text(f"المبلغ المطلوب ({amount:.2f} {currency}) يتجاوز رصيدك ({affiliate_balance:.2f} {currency}). يرجى إدخال مبلغ أقل أو يساوي رصيدك.")
        return WITHDRAWAL_AMOUNT
    if amount < MIN_WITHDRAWAL_AMOUNT:
        await tg_update.message.reply_text(f"المبلغ المطلوب ({amount:.2f} {currency}) أقل من الحد الأدنى للسحب ({MIN_WITHDRAWAL_AMOUNT:.2f} {currency}). يرجى إدخال مبلغ أكبر.")
        return WITHDRAWAL_AMOUNT
    context.user_data['withdrawal_amount'] = amount
    await tg_update.message.reply_text("أدخل رقم الهاتف الذي سيتم تحويل المبلغ إليه (يجب أن يكون رقم مصري يبدأ بـ +20):")
    return WITHDRAWAL_PHONE

async def withdrawal_phone(tg_update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    phone = tg_update.message.text.strip()
    if not validate_affiliate_phone(phone): # Withdrawal phone is Egyptian
        await tg_update.message.reply_text("رقم الهاتف غير صالح. يرجى إدخال رقم مصري صحيح يبدأ بـ +20 و10 أرقام بعده.")
        return WITHDRAWAL_PHONE
    
    affiliate_id = context.user_data.get('affiliate_id')
    amount = context.user_data.get('withdrawal_amount')
    currency = context.user_data.get('withdrawal_currency', 'USD')

    async with SessionLocal() as session:
        try:
            result = await session.execute(select(Affiliate).where(Affiliate.id == affiliate_id))
            affiliate = result.scalar_one_or_none()
            if not affiliate:
                await tg_update.message.reply_text("حدث خطأ: لم يتم العثور على حساب المسوّق الخاص بك. يرجى المحاولة مرة أخرى.", reply_markup=main_menu())
                context.user_data.clear()
                return ConversationHandler.END
            
            if amount > affiliate.balance: # Re-check balance just in case
                await tg_update.message.reply_text(
                    f"المبلغ المطلوب ({amount:.2f} {currency}) يتجاوز رصيدك الحالي ({affiliate.balance:.2f} {currency}). يرجى المحاولة مرة أخرى بمبلغ أقل.",
                    reply_markup=main_menu()
                )
                context.user_data.clear()
                return ConversationHandler.END
            
            # Check for pending withdrawals again before creating to prevent duplicates if user spams
            pending_withdrawals = await session.execute(
                select(Withdrawal).where(
                    Withdrawal.affiliate_id == affiliate.id,
                    Withdrawal.status == "pending"
                )
            )
            if pending_withdrawals.scalars().first():
                await tg_update.message.reply_text("لديك بالفعل طلب سحب قيد المراجعة. تم إلغاء طلبك الجديد.", reply_markup=main_menu())
                context.user_data.clear()
                return ConversationHandler.END

            withdrawal = Withdrawal(
                affiliate_id=affiliate.id,
                amount=amount,
                phone=phone,
                currency=currency,
                requested_at=get_now_timezone_aware()
            )
            session.add(withdrawal)

            # Do NOT subtract balance here. Balance will be subtracted once approved by admin.
            # This is a change from your original logic to reflect common practice.
            # await session.execute(
            #     update(Affiliate)
            #     .where(Affiliate.id == affiliate.id)
            #     .values(balance=Affiliate.balance - amount)
            # )
            await session.commit()
            await tg_update.message.reply_text(
                f"تم تسجيل طلب السحب بقيمة {amount:.2f} {currency} بنجاح! سيتم المراجعة قريبًا. رصيدك سيتم خصمه عند الموافقة.",
                reply_markup=main_menu()
            )
            logger.info(f"Withdrawal request by {affiliate.name} (ID: {affiliate.id}) for {amount:.2f} {currency} to {phone}")
        except Exception as e:
            await session.rollback()
            logger.error(f"Error processing withdrawal for {tg_update.effective_user.id}: {e}", exc_info=True)
            await tg_update.message.reply_text("حدث خطأ أثناء تسجيل طلب السحب. يرجى المحاولة مرة أخرى.", reply_markup=main_menu())
        finally:
            context.user_data.clear()
            return ConversationHandler.END

async def cmd_balance(tg_update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = tg_update.effective_user.id
    async with SessionLocal() as session:
        result = await session.execute(select(Affiliate).where(Affiliate.telegram_id == user_id))
        affiliate = result.scalar_one_or_none()
        if not affiliate:
            await tg_update.message.reply_text("يرجى التسجيل أولاً باستخدام /start")
            return

        response = (
            f"💰 كشف حساب العمولة\n"
            f"الاسم: {affiliate.name}\n"
            f"المتجر: {affiliate.store_name}\n"
            f"الرصيد الحالي: {affiliate.balance:.2f} USD\n" # Assuming affiliate balance is always USD
            f"إجمالي العمولات: {affiliate.total_earnings:.2f} USD\n"
            f"إجمالي المبيعات: {affiliate.total_sales:.2f} USD\n"
            f"عدد الطلبات: {affiliate.total_orders}\n\n"
        )
        
        # Show pending withdrawals for the user
        pending_withdrawals = await session.execute(
            select(Withdrawal).where(
                Withdrawal.affiliate_id == affiliate.id,
            Withdrawal.status == "pending"
        ).order_by(Withdrawal.requested_at.asc()))
        pending_withdrawals = pending_withdrawals.scalars().all()

        if pending_withdrawals:
            response += "طلبات السحب المعلقة:\n"
            for w in pending_withdrawals:
                response += f"- مبلغ: {w.amount:.2f} {w.currency} | رقم الهاتف: {w.phone} | طلب في: {w.requested_at.strftime('%Y-%m-%d %H:%M')}\n"
        
        await tg_update.message.reply_text(response)

async def admin_command(tg_update: Update, context: ContextTypes.DEFAULT_TYPE):
    if tg_update.effective_user.id not in ADMIN_IDS:
        await tg_update.message.reply_text("غير مصرح لك باستخدام هذا الأمر.")
        logger.warning(f"Unauthorized admin access attempt by {tg_update.effective_user.id}")
        return ConversationHandler.END # End conversation if not admin
    await tg_update.message.reply_text("قائمة المدير:", reply_markup=admin_menu())
    return ADMIN_MENU # Stay in admin menu state

async def cmd_stats(tg_update: Update, context: ContextTypes.DEFAULT_TYPE):
    if tg_update.effective_user.id not in ADMIN_IDS:
        await tg_update.message.reply_text("غير مصرح لك باستخدام هذا الأمر.")
        return ConversationHandler.END

    async with SessionLocal() as session:
        result = await session.execute(select(Affiliate).order_by(Affiliate.total_sales.desc()))
        affiliates = result.scalars().all()

        if not affiliates:
            await tg_update.message.reply_text("لا يوجد مسوّقين مسجلين حتى الآن.", reply_markup=admin_menu())
            return ADMIN_MENU

        for affiliate in affiliates:
            delivered_count = await session.execute(
                select(func.count()).select_from(Order).where(Order.affiliate_id == affiliate.id, Order.status == "delivered")
            )
            delivered_count = delivered_count.scalar_one()

            response = (
                f"👤 {affiliate.name} ({affiliate.store_name})\n"
                f"  رصيد: {affiliate.balance:.2f} USD\n"
                f"  إجمالي العمولات: {affiliate.total_earnings:.2f} USD\n"
                f"  مبيعات: {affiliate.total_sales:.2f} USD\n"
                f"  طلبات كلية: {affiliate.total_orders}\n"
                f"  طلبات مكتملة: {delivered_count}\n\n"
            )
            
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("عرض الطلبات", callback_data=f"view_orders_{affiliate.id}")]
            ])
            await tg_update.message.reply_text(response, reply_markup=keyboard)
        
        # Add recent pending withdrawals for admin overview
        pending_withdrawals = await session.execute(
            select(Withdrawal).where(
                Withdrawal.status == "pending"
            ).order_by(Withdrawal.requested_at.asc()).limit(5)
        )
        recent_pending_withdrawals = pending_withdrawals.scalars().all()

        if recent_pending_withdrawals:
            response = "\n\n💵 آخر 5 طلبات سحب معلقة:\n"
            for w in recent_pending_withdrawals:
                # Fetch affiliate name for each withdrawal
                affiliate_name_res = await session.execute(select(Affiliate.name).where(Affiliate.id == w.affiliate_id))
                affiliate_name = affiliate_name_res.scalar_one_or_none()
                response += f"- المسوّق: {affiliate_name or 'غير معروف'} | مبلغ: {w.amount:.2f} {w.currency} | هاتف: {w.phone}\n"
            await tg_update.message.reply_text(response)
        
        await tg_update.message.reply_text("انتهت إحصاءات المسوّقين.", reply_markup=admin_menu())
        return ADMIN_MENU

async def handle_view_orders_callback(tg_update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = tg_update.callback_query
    await query.answer()

    if query.from_user.id not in ADMIN_IDS:
        await query.edit_message_text("غير مصرح لك بتنفيذ هذا الإجراء.")
        return ADMIN_MENU

    aff_id = int(query.data.split("_")[2])
    async with SessionLocal() as session:
        affiliate_res = await session.execute(select(Affiliate.name).where(Affiliate.id == aff_id))
        affiliate_name = affiliate_res.scalar_one_or_none()
        if not affiliate_name:
            await query.message.reply_text("لم يتم العثور على المسوّق.", reply_markup=admin_menu())
            return ADMIN_MENU

        orders_res = await session.execute(select(Order).where(Order.affiliate_id == aff_id).order_by(Order.created_at.desc()))
        orders = orders_res.scalars().all()
        if not orders:
            await query.message.reply_text(f"لا توجد طلبات للمسوّق {affiliate_name}.", reply_markup=admin_menu())
            return ADMIN_MENU

        response = f"📦 طلبات المسوّق {affiliate_name} ({len(orders)}):\n\n"
        for order in orders[:20]:
            status_text = "تم التوصيل" if order.status == "delivered" else "في الانتظار" if order.status == "pending" else "هناك مشكلة - تواصل مع الدعم"
            response += (
                f"🆔 {order.id} | العميل: {order.customer_name} | "
                f"العنوان: {order.address}, {order.city} ({order.country}) | المنتج: {order.product} | "
                f"كود: {order.product_code} | الأصلي: {order.cost_price:.2f} {order.currency} | البيع: {order.selling_price:.2f} {order.currency}\n"
                f"  الحالة: {status_text}\n"
            )
        if len(orders) > 20:
            response += "\n... والمزيد."
        await query.message.reply_text(response, reply_markup=admin_menu())
    return ADMIN_MENU

async def cmd_all_orders_admin(tg_update: Update, context: ContextTypes.DEFAULT_TYPE):
    if tg_update.effective_user.id not in ADMIN_IDS:
        await tg_update.message.reply_text("غير مصرح لك باستخدام هذا الأمر.")
        return ConversationHandler.END
    async with SessionLocal() as session:
        result = await session.execute(select(Order).order_by(Order.created_at.desc()))
        orders = result.scalars().all()
        if not orders:
            await tg_update.message.reply_text("لا توجد طلبات مسجلة.")
            return ADMIN_MENU
        response = f"📦 جميع الطلبات ({len(orders)}):\n\n"
        for order in orders[:20]:  # Limit to 20 for brevity
            status_text = "تم التوصيل" if order.status == "delivered" else "في الانتظار" if order.status == "pending" else "هناك مشكلة - تواصل مع الدعم"
            response += (
                f"🆔 {order.id} | المسوّق ID: {order.affiliate_id} | العميل: {order.customer_name} | "
                f"العنوان: {order.address}, {order.city} ({order.country}) | المنتج: {order.product} | "
                f"كود: {order.product_code} | الأصلي: {order.cost_price:.2f} {order.currency} | البيع: {order.selling_price:.2f} {order.currency}\n"
                f"  الحالة: {status_text}\n"
            )
        if len(orders) > 20:
            response += "\n... والمزيد."
        await tg_update.message.reply_text(response, reply_markup=admin_menu())
        return ADMIN_MENU

async def admin_manage_orders(tg_update: Update, context: ContextTypes.DEFAULT_TYPE):
    if tg_update.effective_user.id not in ADMIN_IDS:
        await tg_update.message.reply_text("غير مصرح لك باستخدام هذا الأمر.")
        return ConversationHandler.END
    
    await show_pending_orders(tg_update, context)
    return ADMIN_ORDERS_MENU

async def show_pending_orders(tg_update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with SessionLocal() as session:
        pending_orders = await session.execute(
            select(Order).where(Order.status == "pending").order_by(Order.created_at.asc())
        )
        orders = pending_orders.scalars().all()

        if not orders:
            await tg_update.effective_message.reply_text("لا توجد طلبات معلقة حالياً.", reply_markup=admin_menu())
            return ConversationHandler.END # If no orders, go back to admin menu
        
        response = "🛠 طلبات الطلبات المعلقة:\n\n"
        for order in orders:
            affiliate_res = await session.execute(select(Affiliate).where(Affiliate.id == order.affiliate_id))
            affiliate = affiliate_res.scalar_one_or_none()
            affiliate_name = affiliate.name if affiliate else "غير معروف"

            keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("تم التوصيل", callback_data=f"delivered_{order.id}"),
                    InlineKeyboardButton("هناك مشكلة", callback_data=f"issue_{order.id}")
                ]
            ])
            response = (
                f"----------------------------------------\n"
                f"🆔 طلب #{order.id}\n"
                f"  المسوّق: {affiliate_name}\n"
                f"  العميل: {order.customer_name} | هاتف: {order.customer_phone}\n"
                f"  العنوان: {order.address}, {order.city} ({order.country})\n"
                f"  المنتج: {order.product} | كود: {order.product_code}\n"
                f"  سعر الأصلي: {order.cost_price:.2f} {order.currency} | سعر البيع: {order.selling_price:.2f} {order.currency}\n"
                f"  العمولة المحتملة: {convert_to_usd(order.commission, order.currency):.2f} USD\n"
                f"  تاريخ الطلب: {order.created_at.strftime('%Y-%m-%d %H:%M')}\n"
                f"----------------------------------------\n"
            )
            await tg_update.effective_message.reply_text(response, reply_markup=keyboard)

        await tg_update.effective_message.reply_text("انتهت قائمة الطلبات المعلقة. اختر من القائمة:", reply_markup=admin_menu())
        return ADMIN_ORDERS_MENU

async def handle_order_status_callback(tg_update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = tg_update.callback_query
    await query.answer() # Acknowledge the callback

    if query.from_user.id not in ADMIN_IDS:
        await query.edit_message_text("غير مصرح لك بتنفيذ هذا الإجراء.")
        return

    action, order_id = query.data.split('_')
    order_id = int(order_id)

    async with SessionLocal() as session:
        order_res = await session.execute(select(Order).where(Order.id == order_id))
        order = order_res.scalar_one_or_none()

        if not order:
            await query.edit_message_text(f"خطأ: لم يتم العثور على الطلب رقم {order_id}.", reply_markup=admin_menu())
            return ADMIN_MENU
        
        if order.status != "pending":
            await query.edit_message_text(f"الطلب رقم {order_id} تمت معالجته بالفعل ({order.status}).", reply_markup=admin_menu())
            return ADMIN_MENU

        affiliate_res = await session.execute(select(Affiliate).where(Affiliate.id == order.affiliate_id))
        affiliate = affiliate_res.scalar_one_or_none()
        
        if not affiliate:
            await query.edit_message_text(f"خطأ: لم يتم العثور على المسوّق للطلب رقم {order_id}.", reply_markup=admin_menu())
            return ADMIN_MENU

        usd_commission = convert_to_usd(order.commission, order.currency)
        usd_selling_price = convert_to_usd(order.selling_price, order.currency)

        if action == "delivered":
            order.status = "delivered"
            await session.execute(
                update(Affiliate)
                .where(Affiliate.id == affiliate.id)
                .values(
                    balance=Affiliate.balance + usd_commission,
                    total_earnings=Affiliate.total_earnings + usd_commission,
                    total_sales=Affiliate.total_sales + usd_selling_price
                )
            )
            await session.commit()
            await query.edit_message_text(f"✅ تم تأكيد توصيل الطلب رقم {order_id} بنجاح.\nتم إضافة {usd_commission:.2f} USD إلى رصيد المسوّق {affiliate.name}.", reply_markup=admin_menu())
            logger.info(f"Admin {query.from_user.id} confirmed delivery for order {order_id} for affiliate {affiliate.id}. Commission: {usd_commission:.2f} USD")
        elif action == "issue":
            order.status = "issue"
            await session.commit()
            await query.edit_message_text(f"❌ تم وضع علامة مشكلة على الطلب رقم {order_id}.", reply_markup=admin_menu())
            logger.info(f"Admin {query.from_user.id} marked issue for order {order_id} for affiliate {affiliate.id}.")
        
        return ADMIN_MENU # Return to admin menu after processing

async def admin_manage_withdrawals(tg_update: Update, context: ContextTypes.DEFAULT_TYPE):
    if tg_update.effective_user.id not in ADMIN_IDS:
        await tg_update.message.reply_text("غير مصرح لك باستخدام هذا الأمر.")
        return ConversationHandler.END
    
    await show_pending_withdrawals(tg_update, context)
    return ADMIN_WITHDRAWALS_MENU

async def show_pending_withdrawals(tg_update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with SessionLocal() as session:
        pending_withdrawals = await session.execute(
            select(Withdrawal).where(Withdrawal.status == "pending").order_by(Withdrawal.requested_at.asc())
        )
        withdrawals = pending_withdrawals.scalars().all()

        if not withdrawals:
            await tg_update.effective_message.reply_text("لا توجد طلبات سحب معلقة حالياً.", reply_markup=admin_menu())
            return ConversationHandler.END # If no withdrawals, go back to admin menu
        
        response = "💵 طلبات السحب المعلقة:\n\n"
        for w in withdrawals:
            affiliate_res = await session.execute(select(Affiliate).where(Affiliate.id == w.affiliate_id))
            affiliate = affiliate_res.scalar_one_or_none()
            affiliate_name = affiliate.name if affiliate else "غير معروف"

            keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("✅ موافقة", callback_data=f"approve_{w.id}"),
                    InlineKeyboardButton("❌ رفض", callback_data=f"reject_{w.id}")
                ]
            ])
            response = (
                f"----------------------------------------\n"
                f"🆔 طلب سحب #{w.id}\n"
                f"  المسوّق: {affiliate_name}\n"
                f"  المبلغ: {w.amount:.2f} {w.currency}\n"
                f"  هاتف المسوّق: {w.phone}\n"
                f"  تاريخ الطلب: {w.requested_at.strftime('%Y-%m-%d %H:%M')}\n"
                f"----------------------------------------\n"
            )
            await tg_update.effective_message.reply_text(response, reply_markup=keyboard)

        await tg_update.effective_message.reply_text("انتهت قائمة طلبات السحب المعلقة. اختر من القائمة:", reply_markup=admin_menu())
        return ADMIN_WITHDRAWALS_MENU

async def handle_withdrawal_callback(tg_update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = tg_update.callback_query
    await query.answer() # Acknowledge the callback

    if query.from_user.id not in ADMIN_IDS:
        await query.edit_message_text("غير مصرح لك بتنفيذ هذا الإجراء.")
        return

    action, withdrawal_id = query.data.split('_')
    withdrawal_id = int(withdrawal_id)
    admin_id = query.from_user.id

    async with SessionLocal() as session:
        withdrawal_res = await session.execute(select(Withdrawal).where(Withdrawal.id == withdrawal_id))
        withdrawal = withdrawal_res.scalar_one_or_none()

        if not withdrawal:
            await query.edit_message_text(f"خطأ: لم يتم العثور على طلب السحب رقم {withdrawal_id}.", reply_markup=admin_menu())
            return ADMIN_MENU
        
        if withdrawal.status != "pending":
            await query.edit_message_text(f"طلب السحب رقم {withdrawal_id} تمت معالجته بالفعل ({withdrawal.status}).", reply_markup=admin_menu())
            return ADMIN_MENU

        affiliate_res = await session.execute(select(Affiliate).where(Affiliate.id == withdrawal.affiliate_id))
        affiliate = affiliate_res.scalar_one_or_none()
        
        if not affiliate:
            await query.edit_message_text(f"خطأ: لم يتم العثور على المسوّق لطلب السحب رقم {withdrawal_id}.", reply_markup=admin_menu())
            return ADMIN_MENU

        if action == "approve":
            # Deduct balance only upon approval
            if affiliate.balance < withdrawal.amount:
                await query.edit_message_text(
                    f"لا يمكن الموافقة على طلب السحب رقم {withdrawal_id}: رصيد المسوّق غير كافٍ ({affiliate.balance:.2f} {withdrawal.currency}).",
                    reply_markup=admin_menu()
                )
                return ADMIN_MENU
            
            await session.execute(
                update(Affiliate)
                .where(Affiliate.id == affiliate.id)
                .values(balance=Affiliate.balance - withdrawal.amount)
            )
            withdrawal.status = "approved"
            withdrawal.processed_at = get_now_timezone_aware()
            withdrawal.processed_by_admin_id = admin_id
            await session.commit()
            await query.edit_message_text(f"✅ تم الموافقة على طلب السحب رقم {withdrawal_id} بنجاح.\nخصم {withdrawal.amount:.2f} {withdrawal.currency} من رصيد المسوّق {affiliate.name}.", reply_markup=admin_menu())
            logger.info(f"Admin {admin_id} approved withdrawal {withdrawal_id} for affiliate {affiliate.id}. Amount: {withdrawal.amount:.2f} {withdrawal.currency}")
        elif action == "reject":
            withdrawal.status = "rejected"
            withdrawal.processed_at = get_now_timezone_aware()
            withdrawal.processed_by_admin_id = admin_id
            await session.commit()
            await query.edit_message_text(f"❌ تم رفض طلب السحب رقم {withdrawal_id}.", reply_markup=admin_menu())
            logger.info(f"Admin {admin_id} rejected withdrawal {withdrawal_id} for affiliate {affiliate.id}.")
        
        return ADMIN_MENU # Return to admin menu after processing

async def cmd_export(tg_update: Update, context: ContextTypes.DEFAULT_TYPE):
    if tg_update.effective_user.id not in ADMIN_IDS:
        await tg_update.message.reply_text("غير مصرح لك باستخدام هذا الأمر.")
        return ConversationHandler.END

    await tg_update.message.reply_text("جاري إعداد ملف التصدير، يرجى الانتظار...")

    excel_path = None
    try:
        # Use a synchronous connection for pandas read_sql_query
        async with engine.connect() as conn:
            # Need to get a sync connection from the async one
            sync_conn = await conn.get_sync_connection()
            affiliates_df = pd.read_sql_query(select(Affiliate).statement, sync_conn)
            
            orders_query: Select = select(Order.__table__.c, label("affiliate_name", Affiliate.name)).join(Affiliate, Order.affiliate_id == Affiliate.id)
            orders_df = pd.read_sql_query(orders_query, sync_conn)
            
            withdrawals_query: Select = select(Withdrawal.__table__.c, label("affiliate_name", Affiliate.name)).join(Affiliate, Withdrawal.affiliate_id == Affiliate.id)
            withdrawals_df = pd.read_sql_query(withdrawals_query, sync_conn)

        timestamp = get_now_timezone_aware().strftime("%Y%m%d_%H%M%S")
        export_filename = f"export_{timestamp}.xlsx"
        excel_path = os.path.join(EXPORT_DIR, export_filename)

        with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
            affiliates_df.to_excel(writer, sheet_name='Affiliates', index=False)
            orders_df.to_excel(writer, sheet_name='Orders', index=False)
            withdrawals_df.to_excel(writer, sheet_name='Withdrawals', index=False)

        with open(excel_path, 'rb') as f:
            await tg_update.message.reply_document(document=f, filename=export_filename)
        logger.info(f"Exported data to {export_filename} for admin {tg_update.effective_user.id}")

    except Exception as e:
        logger.error(f"Error during export for admin {tg_update.effective_user.id}: {e}", exc_info=True)
        await tg_update.message.reply_text("حدث خطأ أثناء عملية التصدير. يرجى المحاولة مرة أخرى لاحقًا.", reply_markup=admin_menu())
    finally:
        if excel_path and os.path.exists(excel_path):
            try:
                os.remove(excel_path)
            except OSError as e:
                logger.warning(f"Error removing excel file {excel_path}: {e}")
        return ADMIN_MENU # Return to admin menu

async def cmd_back_to_main_menu(tg_update: Update, context: ContextTypes.DEFAULT_TYPE):
    await tg_update.message.reply_text("العودة إلى القائمة الرئيسية:", reply_markup=main_menu())
    context.user_data.clear() # Clear user data on returning to main menu
    return ConversationHandler.END

# --- Conversation Handlers ---
registration_conv_handler = ConversationHandler(
    entry_points=[CommandHandler("start", start_command)],
    states={
        REGISTER_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, register_name)],
        REGISTER_PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, register_phone)],
        REGISTER_STORE_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, register_store_name)],
    },
    fallbacks=[CommandHandler("cancel", cancel_conversation), MessageHandler(filters.Regex("^إلغاء$"), cancel_conversation)],
    allow_reentry=True
)

order_conv_handler = ConversationHandler(
    entry_points=[MessageHandler(filters.Regex("^🛒 طلب أوردر$"), start_order)],
    states={
        ORDER_CUSTOMER_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, order_customer_name)],
        ORDER_COUNTRY: [MessageHandler(filters.TEXT & ~filters.COMMAND, order_country)],
        ORDER_CUSTOMER_PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, order_customer_phone)],
        ORDER_ADDRESS: [MessageHandler(filters.TEXT & ~filters.COMMAND, order_address)],
        ORDER_CITY: [MessageHandler(filters.TEXT & ~filters.COMMAND, order_city)],
        ORDER_PRODUCT: [MessageHandler(filters.TEXT & ~filters.COMMAND, order_product)],
        ORDER_PRODUCT_CODE: [MessageHandler(filters.TEXT & ~filters.COMMAND, order_product_code)],
        ORDER_COST_PRICE: [MessageHandler(filters.TEXT & ~filters.COMMAND, order_cost_price)],
        ORDER_SELLING_PRICE: [MessageHandler(filters.TEXT & ~filters.COMMAND, order_selling_price)],
    },
    fallbacks=[CommandHandler("cancel", cancel_conversation), MessageHandler(filters.Regex("^إلغاء$"), cancel_conversation)],
    allow_reentry=True
)

withdrawal_conv_handler = ConversationHandler(
    entry_points=[MessageHandler(filters.Regex("^💳 طلب سحب$"), start_withdrawal)],
    states={
        WITHDRAWAL_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, withdrawal_amount)],
        WITHDRAWAL_PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, withdrawal_phone)],
    },
    fallbacks=[CommandHandler("cancel", cancel_conversation), MessageHandler(filters.Regex("^إلغاء$"), cancel_conversation)],
    allow_reentry=True
)

admin_conv_handler = ConversationHandler(
    entry_points=[CommandHandler("admin", admin_command)],
    states={
        ADMIN_MENU: [
            MessageHandler(filters.Regex("^📊 إحصاءات المسوّقين$"), cmd_stats),
            MessageHandler(filters.Regex("^📦 عرض جميع الطلبات$"), cmd_all_orders_admin),
            MessageHandler(filters.Regex("^🛠 إدارة الطلبات$"), admin_manage_orders),
            MessageHandler(filters.Regex("^💵 إدارة طلبات السحب$"), admin_manage_withdrawals),
            MessageHandler(filters.Regex("^📁 تصدير شامل \\(Excel\\)$"), cmd_export),
            MessageHandler(filters.Regex("^🔙 العودة إلى القائمة الرئيسية$"), cmd_back_to_main_menu),
            CallbackQueryHandler(handle_view_orders_callback, pattern="^view_orders_(\\d+)$")
        ],
        ADMIN_WITHDRAWALS_MENU: [
            CallbackQueryHandler(handle_withdrawal_callback, pattern="^(approve|reject)_(\\d+)$"),
            MessageHandler(filters.Regex("^🔙 العودة إلى القائمة الرئيسية$"), cmd_back_to_main_menu),
            MessageHandler(filters.TEXT, admin_manage_withdrawals) # If admin sends text while in withdrawals menu, re-show withdrawals
        ],
        ADMIN_ORDERS_MENU: [
            CallbackQueryHandler(handle_order_status_callback, pattern="^(delivered|issue)_(\\d+)$"),
            MessageHandler(filters.Regex("^🔙 العودة إلى القائمة الرئيسية$"), cmd_back_to_main_menu),
            MessageHandler(filters.TEXT, admin_manage_orders) # If admin sends text while in orders menu, re-show orders
        ]
    },
    fallbacks=[CommandHandler("cancel", cancel_conversation), MessageHandler(filters.Regex("^🔙 العودة إلى القائمة الرئيسية$"), cmd_back_to_main_menu)],
    allow_reentry=True
)

# --- Main Execution ---
async def post_init(application: Application):
    await init_db() # Ensure database is initialized when bot starts
    logger.info("Database initialized successfully after bot startup.")

def main():
    logger.info("Starting bot application...")

    application = Application.builder().token(BOT_TOKEN).post_init(post_init).build()

    # User Handlers
    application.add_handler(registration_conv_handler)
    application.add_handler(order_conv_handler)
    application.add_handler(withdrawal_conv_handler)

    application.add_handler(MessageHandler(filters.Regex("^📦 طلباتي السابقة$"), cmd_my_orders))
    application.add_handler(MessageHandler(filters.Regex("^💰 كشف حساب العمولة$"), cmd_balance))
    application.add_handler(MessageHandler(filters.Regex("^🔙 العودة إلى القائمة الرئيسية$"), cmd_back_to_main_menu)) # For non-admin exit


    # Admin Handlers (now part of a conversation for better state management)
    application.add_handler(admin_conv_handler)
    
    # Generic message handler for anything not caught by conversations/commands (should be after specific handlers)
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, unknown_message))

    try:
        application.run_polling(allowed_updates=Update.ALL_TYPES)
    except Exception as e:
        logger.critical(f"Bot polling failed: {e}", exc_info=True)
    finally:
        logger.info("Bot application stopped.")

async def unknown_message(tg_update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle messages that don't match any other handler."""
    # Only reply if it's not part of an active conversation state that handles text
    if not context.application.handlers[0][0].check_update(tg_update) and \
       not context.application.handlers[0][1].check_update(tg_update) and \
       not context.application.handlers[0][2].check_update(tg_update) and \
       not context.application.handlers[0][3].check_update(tg_update): # Check all conv handlers
        await tg_update.message.reply_text("عذرًا، لم أفهم طلبك. يرجى اختيار من القائمة الرئيسية.", reply_markup=main_menu())


if __name__ == "__main__":
    main()