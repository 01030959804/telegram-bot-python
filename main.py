import os
import asyncio
import logging
from datetime import datetime, timedelta
import pytz
import re
import pandas as pd
from sqlalchemy.future import select
from sqlalchemy import Column, Integer, String, Float, DateTime, update, func, label
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.sql import Select
from dotenv import load_dotenv
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
TIMEZONE = os.getenv("TIMEZONE", "Asia/Riyadh")
EXPORT_DIR = os.getenv("EXPORT_DIR", "/app/exports")
RATE_LIMIT_PER_MINUTE = int(os.getenv("RATE_LIMIT_PER_MINUTE", 10))
MIN_WITHDRAWAL_AMOUNT = float(os.getenv("MIN_WITHDRAWAL_AMOUNT", 10.0))
COMMISSION_RATE = float(os.getenv("COMMISSION_RATE", 0.1))
DATABASE_URL = os.getenv("DATABASE_URL")

# Exchange rates to USD (as of 2025-09-07)
SAR_TO_USD = 0.2665
AED_TO_USD = 0.2723

if not BOT_TOKEN:
    logger.critical("BOT_TOKEN is not set. Please set it in the .env file.")
    exit(1)
if not DATABASE_URL:
    logger.critical("DATABASE_URL is not set. Please set it in the .env file.")
    exit(1)

os.makedirs(EXPORT_DIR, exist_ok=True)

# --- Database Setup ---
Base = declarative_base()

class Affiliate(Base):
    __tablename__ = "affiliates"
    id = Column(Integer, primary_key=True)
    telegram_id = Column(Integer, unique=True, nullable=False)
    name = Column(String, nullable=False)
    phone = Column(String, nullable=False)
    store_name = Column(String, nullable=False)
    balance = Column(Float, default=0.0, nullable=False)
    total_earnings = Column(Float, default=0.0, nullable=False)
    total_sales = Column(Float, default=0.0, nullable=False)
    total_orders = Column(Integer, default=0, nullable=False)

class Order(Base):
    __tablename__ = "orders"
    id = Column(Integer, primary_key=True)
    affiliate_id = Column(Integer, nullable=False)
    customer_name = Column(String, nullable=False)
    customer_phone = Column(String, nullable=False)
    address = Column(String, nullable=False)
    city = Column(String, nullable=False)
    country = Column(String, nullable=False)
    currency = Column(String, nullable=False)
    product = Column(String, nullable=False)
    product_code = Column(String, nullable=False)
    cost_price = Column(Float, nullable=False)
    selling_price = Column(Float, nullable=False)
    commission = Column(Float, nullable=False)
    status = Column(String, default="pending", nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False)

class Withdrawal(Base):
    __tablename__ = "withdrawals"
    id = Column(Integer, primary_key=True)
    affiliate_id = Column(Integer, nullable=False)
    amount = Column(Float, nullable=False)
    phone = Column(String, nullable=False)
    status = Column(String, default="pending", nullable=False)
    currency = Column(String, nullable=False)
    requested_at = Column(DateTime(timezone=True), nullable=False)
    processed_at = Column(DateTime(timezone=True), nullable=True)
    processed_by_admin_id = Column(Integer, nullable=True)

try:
    engine = create_async_engine(DATABASE_URL, echo=False)
    SessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
except Exception as e:
    logger.critical(f"Failed to create database engine: {e}")
    exit(1)

async def init_db():
    try:
        async with engine.begin() as conn:
            logger.info("Creating database tables if they don't exist.")
            await conn.run_sync(Base.metadata.create_all)
        logger.info("Database tables created successfully.")
    except Exception as e:
        logger.critical(f"Failed to initialize database tables: {e}")
        raise

# --- Utility Functions ---
def get_now_timezone_aware():
    return datetime.now(pytz.timezone(TIMEZONE))

def validate_affiliate_phone(phone: str) -> bool:
    pattern = r"^\+20\d{10}$"
    return bool(re.match(pattern, phone))

def validate_customer_phone(phone: str, country: str) -> bool:
    if country == "المملكة العربية السعودية":
        pattern = r"^\+966\d{9}$"
    elif country == "الإمارات العربية المتحدة":
        pattern = r"^\+971\d{9}$"
    else:
        return False
    return bool(re.match(pattern, phone))

def get_currency_for_country(country: str) -> str:
    return "SAR" if country == "المملكة العربية السعودية" else "AED" if country == "الإمارات العربية المتحدة" else "N/A"

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

# --- Handlers for Registration ---
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    async with SessionLocal() as session:
        result = await session.execute(select(Affiliate).where(Affiliate.telegram_id == user_id))
        affiliate = result.scalar_one_or_none()
        if affiliate:
            await update.message.reply_text("مرحبًا بك مرة أخرى! اختر من القائمة:", reply_markup=main_menu())
            return ConversationHandler.END
        else:
            await update.message.reply_text("مرحبًا! يرجى التسجيل أولاً. أدخل اسمك الكامل:")
            return REGISTER_NAME

async def register_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data['name'] = update.message.text
    await update.message.reply_text("أدخل رقم هاتفك (مثال: +201234567890):")
    return REGISTER_PHONE

async def register_phone(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    phone = update.message.text
    if not validate_affiliate_phone(phone):
        await update.message.reply_text("رقم الهاتف غير صالح. يرجى إدخال رقم مصري صحيح (مثال: +201234567890):")
        return REGISTER_PHONE
    context.user_data['phone'] = phone
    await update.message.reply_text("أدخل اسم متجرك:")
    return REGISTER_STORE_NAME

async def register_store_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    store_name = update.message.text
    user_id = update.effective_user.id
    async with SessionLocal() as session:
        affiliate = Affiliate(
            telegram_id=user_id,
            name=context.user_data['name'],
            phone=context.user_data['phone'],
            store_name=store_name
        )
        session.add(affiliate)
        await session.commit()
    await update.message.reply_text("تم التسجيل بنجاح! اختر من القائمة:", reply_markup=main_menu())
    context.user_data.clear()
    return ConversationHandler.END

# --- Handlers for Orders ---
async def start_order(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    if not await rate_limit_check(user_id):
        await update.message.reply_text("تجاوزت الحد المسموح به للطلبات في الدقيقة. يرجى الانتظار.")
        return ConversationHandler.END
    await update.message.reply_text("أدخل اسم العميل:")
    return ORDER_CUSTOMER_NAME

async def order_customer_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data['customer_name'] = update.message.text
    await update.message.reply_text("اختر البلد:", reply_markup=country_selection_keyboard())
    return ORDER_COUNTRY

async def order_country(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    country = update.message.text
    if country == "إلغاء":
        await update.message.reply_text("تم إلغاء الطلب.", reply_markup=main_menu())
        return ConversationHandler.END
    if country not in ["المملكة العربية السعودية", "الإمارات العربية المتحدة"]:
        await update.message.reply_text("بلد غير صالح. اختر من القائمة:")
        return ORDER_COUNTRY
    context.user_data['country'] = country
    context.user_data['currency'] = get_currency_for_country(country)
    await update.message.reply_text("أدخل رقم هاتف العميل:")
    return ORDER_CUSTOMER_PHONE

async def order_customer_phone(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    phone = update.message.text
    if not validate_customer_phone(phone, context.user_data['country']):
        await update.message.reply_text("رقم الهاتف غير صالح للبلد المختار. أعد الإدخال:")
        return ORDER_CUSTOMER_PHONE
    context.user_data['customer_phone'] = phone
    await update.message.reply_text("أدخل عنوان العميل:")
    return ORDER_ADDRESS

async def order_address(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data['address'] = update.message.text
    await update.message.reply_text("أدخل المدينة:")
    return ORDER_CITY

async def order_city(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data['city'] = update.message.text
    await update.message.reply_text("أدخل اسم المنتج:")
    return ORDER_PRODUCT

async def order_product(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data['product'] = update.message.text
    await update.message.reply_text("أدخل كود المنتج:")
    return ORDER_PRODUCT_CODE

async def order_product_code(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data['product_code'] = update.message.text
    await update.message.reply_text("أدخل سعر التكلفة:")
    return ORDER_COST_PRICE

async def order_cost_price(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        cost_price = float(update.message.text)
    except ValueError:
        await update.message.reply_text("سعر غير صالح. أعد الإدخال:")
        return ORDER_COST_PRICE
    context.user_data['cost_price'] = cost_price
    await update.message.reply_text("أدخل سعر البيع:")
    return ORDER_SELLING_PRICE

async def order_selling_price(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        selling_price = float(update.message.text)
    except ValueError:
        await update.message.reply_text("سعر غير صالح. أعد الإدخال:")
        return ORDER_SELLING_PRICE
    commission = (selling_price - context.user_data['cost_price']) * COMMISSION_RATE
    user_id = update.effective_user.id
    async with SessionLocal() as session:
        result = await session.execute(select(Affiliate).where(Affiliate.telegram_id == user_id))
        affiliate = result.scalar_one()
        order = Order(
            affiliate_id=affiliate.id,
            customer_name=context.user_data['customer_name'],
            customer_phone=context.user_data['customer_phone'],
            address=context.user_data['address'],
            city=context.user_data['city'],
            country=context.user_data['country'],
            currency=context.user_data['currency'],
            product=context.user_data['product'],
            product_code=context.user_data['product_code'],
            cost_price=context.user_data['cost_price'],
            selling_price=selling_price,
            commission=commission,
            created_at=get_now_timezone_aware()
        )
        session.add(order)
        affiliate.total_orders += 1
        affiliate.total_sales += selling_price
        affiliate.total_earnings += commission
        affiliate.balance += commission
        await session.commit()
    await update.message.reply_text(f"تم إضافة الطلب بنجاح! العمولة: {commission:.2f} {context.user_data['currency']}", reply_markup=main_menu())
    context.user_data.clear()
    return ConversationHandler.END

# --- Handlers for Withdrawals ---
async def start_withdrawal(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    async with SessionLocal() as session:
        result = await session.execute(select(Affiliate).where(Affiliate.telegram_id == user_id))
        affiliate = result.scalar_one()
        if affiliate.balance < MIN_WITHDRAWAL_AMOUNT:
            await update.message.reply_text(f"رصيدك الحالي {affiliate.balance:.2f} أقل من الحد الأدنى {MIN_WITHDRAWAL_AMOUNT:.2f}.", reply_markup=main_menu())
            return ConversationHandler.END
    await update.message.reply_text("أدخل المبلغ المراد سحبه:")
    return WITHDRAWAL_AMOUNT

async def withdrawal_amount(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        amount = float(update.message.text)
    except ValueError:
        await update.message.reply_text("مبلغ غير صالح. أعد الإدخال:")
        return WITHDRAWAL_AMOUNT
    user_id = update.effective_user.id
    async with SessionLocal() as session:
        result = await session.execute(select(Affiliate).where(Affiliate.telegram_id == user_id))
        affiliate = result.scalar_one()
        if amount > affiliate.balance or amount < MIN_WITHDRAWAL_AMOUNT:
            await update.message.reply_text("المبلغ غير مناسب. أعد الإدخال:")
            return WITHDRAWAL_AMOUNT
    context.user_data['amount'] = amount
    await update.message.reply_text("أدخل رقم الهاتف للسحب:")
    return WITHDRAWAL_PHONE

async def withdrawal_phone(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    phone = update.message.text
    if not validate_affiliate_phone(phone):
        await update.message.reply_text("رقم هاتف غير صالح. أعد الإدخال:")
        return WITHDRAWAL_PHONE
    user_id = update.effective_user.id
    async with SessionLocal() as session:
        result = await session.execute(select(Affiliate).where(Affiliate.telegram_id == user_id))
        affiliate = result.scalar_one()
        withdrawal = Withdrawal(
            affiliate_id=affiliate.id,
            amount=context.user_data['amount'],
            phone=phone,
            currency= os.getenv("DEFAULT_CURRENCY", "SAR"),  # افتراضي SAR
            requested_at=get_now_timezone_aware()
        )
        session.add(withdrawal)
        affiliate.balance -= context.user_data['amount']
        await session.commit()
    await update.message.reply_text("تم طلب السحب بنجاح! سيتم مراجعته من الإدارة.", reply_markup=main_menu())
    context.user_data.clear()
    return ConversationHandler.END

# --- Handlers for User Commands ---
async def cmd_my_orders(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    async with SessionLocal() as session:
        result = await session.execute(select(Affiliate).where(Affiliate.telegram_id == user_id))
        affiliate = result.scalar_one()
        orders = await session.execute(select(Order).where(Order.affiliate_id == affiliate.id))
        orders = orders.scalars().all()
        if not orders:
            await update.message.reply_text("لا توجد طلبات سابقة.")
            return
        text = "طلباتك السابقة:\n"
        for order in orders:
            text += f"ID: {order.id} - منتج: {order.product} - حالة: {order.status}\n"
        await update.message.reply_text(text)

async def cmd_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    async with SessionLocal() as session:
        result = await session.execute(select(Affiliate).where(Affiliate.telegram_id == user_id))
        affiliate = result.scalar_one()
        text = f"رصيدك: {affiliate.balance:.2f}\nإجمالي العمولات: {affiliate.total_earnings:.2f}\nإجمالي المبيعات: {affiliate.total_sales:.2f}\nعدد الطلبات: {affiliate.total_orders}"
        await update.message.reply_text(text)

# --- Admin Handlers ---
async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("غير مصرح لك.")
        return ConversationHandler.END
    await update.message.reply_text("مرحبا بالإدارة! اختر:", reply_markup=admin_menu())
    return ADMIN_MENU

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with SessionLocal() as session:
        total_affiliates = await session.execute(select(func.count(Affiliate.id)))
        total_orders = await session.execute(select(func.count(Order.id)))
        total_sales = await session.execute(select(func.sum(Order.selling_price)))
        text = f"إحصاءات:\nمسوقين: {total_affiliates.scalar()}\nطلبات: {total_orders.scalar()}\nمبيعات: {total_sales.scalar():.2f}"
        await update.message.reply_text(text, reply_markup=admin_menu())

async def cmd_all_orders_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with SessionLocal() as session:
        orders = await session.execute(select(Order))
        orders = orders.scalars().all()
        if not orders:
            await update.message.reply_text("لا توجد طلبات.")
            return
        text = "جميع الطلبات:\n"
        for order in orders:
            text += f"ID: {order.id} - مسوق ID: {order.affiliate_id} - حالة: {order.status}\n"
        await update.message.reply_text(text, reply_markup=admin_menu())

async def admin_manage_orders(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("أدخل ID الطلب لإدارته:")
    return ADMIN_ORDERS_MENU

async def handle_order_status_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    action, order_id = query.data.split('_')
    order_id = int(order_id)
    async with SessionLocal() as session:
        order = await session.get(Order, order_id)
        if action == "delivered":
            order.status = "delivered"
        elif action == "issue":
            order.status = "issue"
        await session.commit()
    await query.edit_message_text(f"تم تحديث الطلب {order_id} إلى {order.status}.")
    return ADMIN_MENU

async def admin_manage_withdrawals(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    async with SessionLocal() as session:
        withdrawals = await session.execute(select(Withdrawal).where(Withdrawal.status == "pending"))
        withdrawals = withdrawals.scalars().all()
        if not withdrawals:
            await update.message.reply_text("لا توجد طلبات سحب معلقة.", reply_markup=admin_menu())
            return ADMIN_MENU
        for w in withdrawals:
            keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("موافقة", callback_data=f"approve_{w.id}"),
                                              InlineKeyboardButton("رفض", callback_data=f"reject_{w.id}")]])
            await update.message.reply_text(f"طلب سحب ID: {w.id} - مبلغ: {w.amount:.2f} {w.currency}", reply_markup=keyboard)
    return ADMIN_WITHDRAWALS_MENU

async def handle_withdrawal_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    action, withdrawal_id = query.data.split('_')
    withdrawal_id = int(withdrawal_id)
    admin_id = update.effective_user.id
    async with SessionLocal() as session:
        withdrawal = await session.get(Withdrawal, withdrawal_id)
        affiliate = await session.get(Affiliate, withdrawal.affiliate_id)
        if action == "approve":
            withdrawal.status = "approved"
            withdrawal.processed_at = get_now_timezone_aware()
            withdrawal.processed_by_admin_id = admin_id
            await session.commit()
            await query.edit_message_text(f"✅ تمت الموافقة على طلب السحب رقم {withdrawal_id}.", reply_markup=admin_menu())
            logger.info(f"Admin {admin_id} approved withdrawal {withdrawal_id} for affiliate {affiliate.id}. Amount: {withdrawal.amount:.2f} {withdrawal.currency}")
        elif action == "reject":
            withdrawal.status = "rejected"
            withdrawal.processed_at = get_now_timezone_aware()
            withdrawal.processed_by_admin_id = admin_id
            affiliate.balance += withdrawal.amount  # إعادة المبلغ إلى الرصيد
            await session.commit()
            await query.edit_message_text(f"❌ تم رفض طلب السحب رقم {withdrawal_id}.", reply_markup=admin_menu())
            logger.info(f"Admin {admin_id} rejected withdrawal {withdrawal_id} for affiliate {affiliate.id}.")

    return ADMIN_MENU

async def cmd_export(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("غير مصرح لك باستخدام هذا الأمر.")
        return ConversationHandler.END

    await update.message.reply_text("جاري إعداد ملف التصدير، يرجى الانتظار...")

    excel_path = None
    try:
        async with engine.connect() as conn:
            sync_conn = await conn.get_sync_connection()

            affiliates_df = pd.read_sql_query(select(Affiliate).statement, sync_conn)
            
            orders_query = select(Order.__table__.c, label("affiliate_name", Affiliate.name)).join(Affiliate, Order.affiliate_id == Affiliate.id)
            orders_df = pd.read_sql_query(orders_query, sync_conn)
            
            withdrawals_query = select(Withdrawal.__table__.c, label("affiliate_name", Affiliate.name)).join(Affiliate, Withdrawal.affiliate_id == Affiliate.id)
            withdrawals_df = pd.read_sql_query(withdrawals_query, sync_conn)

        timestamp = get_now_timezone_aware().strftime("%Y%m%d_%H%M%S")
        export_filename = f"export_{timestamp}.xlsx"
        excel_path = os.path.join(EXPORT_DIR, export_filename)

        with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
            affiliates_df.to_excel(writer, sheet_name='Affiliates', index=False)
            orders_df.to_excel(writer, sheet_name='Orders', index=False)
            withdrawals_df.to_excel(writer, sheet_name='Withdrawals', index=False)

        with open(excel_path, 'rb') as f:
            await update.message.reply_document(document=f, filename=export_filename)
        logger.info(f"Exported data to {export_filename} for admin {update.effective_user.id}")

    except Exception as e:
        logger.error(f"Error during export: {e}", exc_info=True)
        await update.message.reply_text("حدث خطأ أثناء التصدير.", reply_markup=admin_menu())
    finally:
        if excel_path and os.path.exists(excel_path):
            os.remove(excel_path)
        return ADMIN_MENU

async def cmd_back_to_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("العودة إلى القائمة الرئيسية:", reply_markup=main_menu())
    context.user_data.clear()
    return ConversationHandler.END

async def cancel_conversation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("تم الإلغاء.", reply_markup=main_menu())
    context.user_data.clear()
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
        ],
        ADMIN_WITHDRAWALS_MENU: [
            CallbackQueryHandler(handle_withdrawal_callback, pattern="^(approve|reject)_(\\d+)$"),
        ],
        ADMIN_ORDERS_MENU: [
            CallbackQueryHandler(handle_order_status_callback, pattern="^(delivered|issue)_(\\d+)$"),
        ]
    },
    fallbacks=[CommandHandler("cancel", cancel_conversation), MessageHandler(filters.Regex("^🔙 العودة إلى القائمة الرئيسية$"), cmd_back_to_main_menu)],
    allow_reentry=True
)

async def post_init(application: Application):
    await init_db()
    logger.info("Database initialized successfully after bot startup.")

def main():
    logger.info("Starting bot application...")

    application = Application.builder().token(BOT_TOKEN).post_init(post_init).build()

    application.add_handler(registration_conv_handler)
    application.add_handler(order_conv_handler)
    application.add_handler(withdrawal_conv_handler)
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(MessageHandler(filters.Regex("^📦 طلباتي السابقة$"), cmd_my_orders))
    application.add_handler(MessageHandler(filters.Regex("^💰 كشف حساب العمولة$"), cmd_balance))

    application.add_handler(admin_conv_handler)
    
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, unknown_message))

    try:
        application.run_polling(allowed_updates=Update.ALL_TYPES)
    except Exception as e:
        logger.critical(f"Bot polling failed: {e}", exc_info=True)
    finally:
        logger.info("Bot application stopped.")

async def unknown_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("عذرًا، لم أفهم طلبك. يرجى اختيار من القائمة الرئيسية.", reply_markup=main_menu())

if __name__ == "__main__":
    main()
