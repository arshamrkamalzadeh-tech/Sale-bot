"""
بات اعلان فروش SkyTunnel
------------------------
- جدول orders دیتابیس بات فروش رو (فقط خوندنی) چک می‌کنه و برای هر خرید جدید، پیام
  اعلان رو تو کانال(ها) می‌فرسته؛ آیدی خریدار نیمه‌مخفی می‌شه (مثلاً 45****90).
- پیام‌ها با یه اکانت شخصی (پروتکل سروش پلاس، کتابخونه‌ی spluspy) فرستاده می‌شن.
- لاگین اکانت مثل بات فروش، داخل خود سروش انجام می‌شه: به «بات کنترل» (NOTIFIER_BOT_TOKEN)
  پیام /start بده، شماره رو شیر کن، کد تأیید رو با خط‌تیره بفرست (مثلاً 1-2-3-4-5).
  ترمینال لازم نیست.
- سشن لاگین فقط روی دیسک سرور می‌مونه (تو دیتابیس ذخیره نمی‌شه)؛ بعد از هر ریدیپلوی
  بات خودش از ادمین می‌خواد دوباره /start بزنه.
- متن/کانال‌ها/روشن‌خاموش از «پنل وب ← اعلان فروش» (جدول settings) عوض می‌شه.

دستورهای ادمین تو چت بات کنترل: /start (لاگین) ، /status ، /saletest ، /disconnect ، /cancel

env ها (مقدار پیش‌فرض داخل همین فایل ست شده؛ با env می‌شه عوضش کرد):
  NOTIFIER_BOT_TOKEN     توکن بات کنترل (حتماً یه بات جدا، نه توکن بات فروش)
  NOTIFIER_ADMIN_ID      آیدی عددی ادمین (پیش‌فرض همون ادمین بات فروش)
  TURSO_DATABASE_URL, TURSO_AUTH_TOKEN
  NOTIFIER_CHANNEL       کانال(ها)؛ چندتا رو با ویرگول یا فاصله جدا کن (پنل جلوتره)
  NOTIFIER_POLL          فاصله‌ی چک‌کردن سفارش‌ها به ثانیه (پیش‌فرض ۵)
"""
import os
import re
import sys
import json
import glob
import time
import queue
import getpass
import builtins
import inspect
import asyncio
import threading
import html as _html
from datetime import datetime, timedelta, timezone

import requests
import turso_serverless

try:
    import spluspy
except Exception as _e:  # noqa
    spluspy = None
    print("❌ کتابخونه‌ی spluspy نصب نیست:", _e)

try:
    from zoneinfo import ZoneInfo
    TEHRAN = ZoneInfo("Asia/Tehran")
except Exception:
    TEHRAN = timezone(timedelta(hours=3, minutes=30))

# ---------------------------------------------------------------------------
# تنظیمات
# ---------------------------------------------------------------------------
TURSO_DATABASE_URL = os.getenv("TURSO_DATABASE_URL", "libsql://skytunnel-mikekamalzadeh-sys.aws-eu-west-1.turso.io").strip()
TURSO_AUTH_TOKEN = os.getenv("TURSO_AUTH_TOKEN", "eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9.eyJhIjoicnciLCJpYXQiOjE3ODkwNzE5MDcsImlkIjoiMDFhMDhjZmQtMmMwMS03YWViLTlmYzQtOWYzMTkwNzEwMjA0Iiwia2lkIjoiNmltOXd5bGxUd2luNkVHWEJWNHVJUE01ZWNPM2JyZmJ3NzFqWTFIanFCTSIsInJpZCI6ImU5N2NmZmQwLTgxZDYtNDA3Yi1hZGQ4LTEzMTRjY2MyZTkxYyJ9.tZj3mmc_tNCVRPsLkwuWdrGsXZXmFCcvRCEPcN0iclAMk0QrHWfZxZiGqB5otvhlZaT9fs_WzsO6kTS_gnk1Bw").strip()
if not TURSO_DATABASE_URL or not TURSO_AUTH_TOKEN:
    raise SystemExit("TURSO_DATABASE_URL و TURSO_AUTH_TOKEN باید تنظیم بشن.")

NOTIFIER_BOT_TOKEN = os.getenv("NOTIFIER_BOT_TOKEN", "70430157:gnMqA5ro5gy3XvdDVONn0VlmUlZ0uydo3sE").strip()
ADMIN_ID = int(os.getenv("NOTIFIER_ADMIN_ID", "48198481"))
BASE_URL = "https://api.splus.ir/bot" + NOTIFIER_BOT_TOKEN
if not NOTIFIER_BOT_TOKEN:
    raise SystemExit("NOTIFIER_BOT_TOKEN (توکن بات کنترل) باید تنظیم بشه.")

SESSION_NAME = "notifier_session"
CONFIG_PATH = "notifier_config.json"
CODE_TIMEOUT = 300            # چند ثانیه منتظر کد/رمز از ادمین بمونه
POLL = max(2, int(os.getenv("NOTIFIER_POLL", "5") or 5))
HEARTBEAT_EVERY = 60          # هر چند ثانیه وضعیت زنده بودن تو دیتابیس نوشته بشه
MAX_SEND_RETRIES = 3          # بعد از این تعداد شکست، اون سفارش رد می‌شه تا صف گیر نکنه

DEFAULT_TEMPLATE = (
    "📦 {gb} گیگ {type}\n"
    "🕒 در ساعت {time}\n"
    "💰 با قیمت {price} تومان\n"
    "👤 توسط آیدی عددی {user}\n"
    "\n"
    "🛒 جهت خرید: {bot}"
)
DEFAULT_BOT = "@SKYTUNNELbot"
# کانال اعلان فروش از env (NOTIFIER_CHANNEL) یا پنل وب؛ اگه از پنل ثبت بشه، همون جایگزین می‌شه
DEFAULT_CHANNEL = os.getenv("NOTIFIER_CHANNEL", "").strip()
TEMPLATE_FIELDS = ("gb", "type", "days", "price", "time", "date", "user", "bot")

# نام نوع سرویس در اعلان (از پنل قابل تغییره: sale_notify_type_<type>)
DEFAULT_TYPE_NAMES = {
    "config": "کانفیگ",
    "wireguard": "وایرگارد",
    "both": "کانفیگ + وایرگارد",
    "openvpn": "OpenVPN",
    "dns": "DNS بازی",
}

# ---------------------------------------------------------------------------
# دیتابیس
# ---------------------------------------------------------------------------
_db_local = threading.local()


def get_conn():
    conn = getattr(_db_local, "conn", None)
    if conn is None:
        conn = turso_serverless.connect(TURSO_DATABASE_URL, auth_token=TURSO_AUTH_TOKEN)
        conn.close = lambda: None
        _db_local.conn = conn
    return conn


def reset_conn():
    _db_local.conn = None


def with_db_retry(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            msg = str(e).lower()
            if "stream not found" in msg or "stream_expired" in msg or ("404" in msg and "stream" in msg):
                reset_conn()
                return func(*args, **kwargs)
            raise
    wrapper.__name__ = func.__name__
    return wrapper


@with_db_retry
def db_init():
    conn = get_conn()
    c = conn.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)")
    conn.commit()


@with_db_retry
def get_all_settings():
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT key, value FROM settings WHERE key LIKE 'sale_notify_%'")
    return {r[0]: r[1] for r in c.fetchall()}


@with_db_retry
def set_setting(key, value):
    conn = get_conn()
    c = conn.cursor()
    c.execute("INSERT INTO settings (key, value) VALUES (?, ?) "
              "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, str(value)))
    conn.commit()


@with_db_retry
def max_order_id():
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT COALESCE(MAX(id), 0) FROM orders")
    row = c.fetchone()
    return int(row[0]) if row else 0


@with_db_retry
def new_orders(after_id, limit=20):
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT id, user_id, label, gb, days, price, type FROM orders "
              "WHERE id > ? ORDER BY id ASC LIMIT ?", (after_id, limit))
    return c.fetchall()


# ---------------------------------------------------------------------------
# ساخت متن اعلان
# ---------------------------------------------------------------------------
def mask_user_id(uid):
    s = str(uid)
    if len(s) <= 4:
        return s[:1] + "*" * max(1, len(s) - 1)
    return s[:2] + "****" + s[-2:]


def fmt_gb(gb):
    try:
        g = float(gb)
    except Exception:
        return str(gb)
    return str(int(g)) if g == int(g) else ("%g" % g)


def fmt_price(p):
    try:
        return f"{int(p):,}"
    except Exception:
        return str(p)


def render_template(tpl, values):
    """فقط placeholder های شناخته‌شده جایگزین می‌شن؛ آکولاد اضافه هیچ‌وقت خطا نمی‌ده."""
    def rep(m):
        k = m.group(1)
        return str(values[k]) if k in values else m.group(0)
    return re.sub(r"\{(\w+)\}", rep, tpl)


def strip_tags(text):
    return _html.unescape(re.sub(r"<[^>]+>", "", text))


def build_message(settings, order, html_mode=False):
    """order: (id, user_id, label, gb, days, price, type)"""
    _id, user_id, _label, gb, days, price, otype = order
    otype = otype or "both"
    type_name = (settings.get("sale_notify_type_" + otype) or "").strip() \
        or DEFAULT_TYPE_NAMES.get(otype, str(otype))
    now = datetime.now(TEHRAN)
    esc = _html.escape if html_mode else (lambda x: x)
    values = {
        "gb": fmt_gb(gb),
        "type": esc(type_name),
        "days": days if days is not None else "",
        "price": fmt_price(price),
        "time": f"{now.hour:02d}:{now.minute:02d}",
        "date": now.strftime("%Y-%m-%d"),
        "user": mask_user_id(user_id),
        "bot": esc((settings.get("sale_notify_bot") or DEFAULT_BOT).strip()),
    }
    tpl = (settings.get("sale_notify_template") or "").strip() or DEFAULT_TEMPLATE
    return render_template(tpl, values)


SAMPLE_ORDER = (0, 4512345690, "sample", 10, 30, 35000, "config")

# ---------------------------------------------------------------------------
# یوزربات
# ---------------------------------------------------------------------------
async def _maybe_await(x):
    return await x if inspect.isawaitable(x) else x


def parse_channel(raw):
    s = str(raw or "").strip()
    if not s:
        return None
    if re.fullmatch(r"-?\d+", s):
        return int(s)
    s = s.split("?")[0].rstrip("/")
    if "joinchat" in s.lower() or s.split("/")[-1].startswith("+"):
        return None   # لینک دعوت خصوصی پشتیبانی نمی‌شه؛ یوزرنیم کانال یا آیدی عددی بده
    return s.split("/")[-1].lstrip("@") or None


MAX_CHANNELS = 10


def split_channels(raw):
    """چند کانال (هر خط/ویرگول/فاصله یه کانال) → لیست بدون تکراری، به ترتیب."""
    out = []
    for part in re.split(r"[\s,;،]+", str(raw or "")):
        part = part.strip()
        if part and part not in out:
            out.append(part)
    return out[:MAX_CHANNELS]


@with_db_retry
def forget_old_db_session():
    """نسخه‌های قبلی این بات سشن رو تو دیتابیس می‌ذاشتن؛ اگه چیزی مونده، پاکش کن."""
    conn = get_conn()
    c = conn.cursor()
    c.execute("DELETE FROM notifier_storage WHERE name='session'")
    conn.commit()


def write_status(ok, msg, account=None):
    """وضعیت برای کارت «اعلان فروش» تو پنل وب."""
    payload = json.dumps({"ts": datetime.now(timezone.utc).isoformat(), "ok": ok,
                          "msg": msg, "account": account}, ensure_ascii=False)
    try:
        set_setting("sale_notify_status", payload)
    except Exception as e:
        print("write_status error:", repr(e))


# ---------------------------------------------------------------------------
# وضعیت اکانت + Bot API (فقط برای چت با ادمین)
# ---------------------------------------------------------------------------
ST = {"phone": None, "starting": False, "running": False, "waiting": None, "account": None,
      "gen": 0, "client": None, "loop": None, "notifier": None}
ANSWERS = queue.Queue()
TSTATE = {}           # مرحله‌ی «منتظر شماره»
SESSION = requests.Session()


def send_message(chat_id, text, reply_markup=None):
    payload = {"chat_id": chat_id, "text": text}
    if reply_markup:
        payload["reply_markup"] = json.dumps(reply_markup)
    try:
        return SESSION.post(BASE_URL + "/sendMessage", json=payload, timeout=15).json()
    except Exception:
        return {"ok": False}


def delete_message(chat_id, message_id):
    try:
        SESSION.post(BASE_URL + "/deleteMessage", json={"chat_id": chat_id, "message_id": message_id}, timeout=8)
    except Exception:
        pass


def notify_admin(text, markup=None):
    send_message(ADMIN_ID, text, markup)


def get_updates(offset=None):
    params = {"timeout": 25}
    if offset:
        params["offset"] = offset
    try:
        data = SESSION.get(BASE_URL + "/getUpdates", params=params, timeout=35).json()
        if not data.get("ok"):
            print("⚠️ خطای getUpdates:", data)
        return data
    except Exception as e:
        print("⚠️ استثنا در getUpdates:", e)
        return {"ok": False, "result": []}


def contact_kb():
    return {"keyboard": [[{"text": "📱 ارسال شماره من", "request_contact": True}]],
            "resize_keyboard": True, "one_time_keyboard": True}


REMOVE_KB = {"remove_keyboard": True}


def normalize_phone(s):
    d = re.sub(r"\D", "", str(s))
    if d.startswith("00"):
        d = d[2:]
    if d.startswith("0"):
        d = "98" + d[1:]
    elif len(d) == 10 and d.startswith("9"):
        d = "98" + d
    return "+" + d if len(d) >= 10 else None


def cfg_load():
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def cfg_save(cfg):
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False)


def session_files():
    skip = ("-journal", "-wal", "-shm")
    return [f for f in glob.glob(SESSION_NAME + "*") if not f.endswith(skip) and os.path.isfile(f)]


# ---------------- گرفتن کد/رمز لاگین از ادمین ----------------
def ask(kind):
    while not ANSWERS.empty():
        ANSWERS.get_nowait()
    ST["waiting"] = kind
    if kind == "code":
        notify_admin("📩 کد تأیید به سروش پلاسِ اکانتت ارسال شد.\n"
                     "کد رو همین‌جا بفرست — ولی با خط‌تیره بین رقم‌ها، مثلاً 1-2-3-4-5\n"
                     "(اگه کد رو سرراست تو چت بفرستی ممکنه سروش پلاس باطلش کنه)")
    else:
        notify_admin("🔐 اکانتت رمز دومرحله‌ای داره. رمزت رو بفرست:")
    try:
        ans = ANSWERS.get(timeout=CODE_TIMEOUT)
    except queue.Empty:
        ST["waiting"] = None
        raise TimeoutError("از ادمین جوابی نیومد")
    ST["waiting"] = None
    return re.sub(r"\D", "", ans) if kind == "code" else ans


def fake_input(prompt=""):
    p = str(prompt).lower()
    if any(k in p for k in ("phone", "mobile", "number", "شماره")):
        return ST["phone"] or ""
    if any(k in p for k in ("pass", "2fa", "رمز")):
        return ask("password")
    return ask("code")


if spluspy is not None:
    builtins.input = fake_input
    getpass.getpass = lambda prompt="", stream=None: fake_input(prompt)


# ---------------------------------------------------------------------------
# اعلان‌دهنده (داخل ترد یوزربات اجرا می‌شه)
# ---------------------------------------------------------------------------
class Notifier:
    def __init__(self, client, gen, account=None):
        self.client = client
        self.gen = gen
        self.account = account
        self.entities = {}              # channel -> entity
        self.fail_count = {}            # order_id -> تعداد شکست
        self.last_heartbeat = 0.0
        self.last_status = None
        self.hold_until = 0.0           # تا این لحظه نتیجه‌ی تست تو پنل می‌مونه

    # ---- ارسال ----
    async def get_entity(self, channel):
        ent = self.entities.get(channel)
        if ent is not None:
            return ent
        target = parse_channel(channel)
        if target is None:
            raise ValueError("کانال نامعتبره (یوزرنیم یا آیدی عددی کانال رو بده؛ لینک دعوت خصوصی پشتیبانی نمی‌شه)")
        getter = getattr(self.client, "get_entity", None) or getattr(self.client, "get_chat")
        ent = await _maybe_await(getter(target))
        self.entities[channel] = ent
        return ent

    async def send(self, channel, text_html, settings=None):
        ent = await self.get_entity(channel)
        try:
            await _maybe_await(self.client.send_message(ent, strip_tags(text_html)))
        except Exception:
            self.entities.pop(channel, None)   # دفعه‌ی بعد دوباره resolve بشه
            raise

    async def send_all(self, channels, text_html, settings=None):
        """به همه‌ی کانال‌ها می‌فرسته؛ (لیست موفق‌ها، دیکشنری خطاها) برمی‌گردونه."""
        ok, failed = [], {}
        for ch in channels:
            try:
                await self.send(ch, text_html, settings)
                ok.append(ch)
            except Exception as e:
                print(f"[send] کانال {ch} ناموفق:", repr(e))
                failed[ch] = repr(e)[:150]
        return ok, failed

    async def do_test(self, settings, channels):
        return await self.send_all(
            channels, "🧪 تست اعلان فروش\n\n" + build_message(settings, SAMPLE_ORDER, True), settings)

    # ---- وضعیت برای پنل ----
    async def report(self, ok, msg, force=False):
        now = time.time()
        key = (ok, msg)
        if not force and key == self.last_status and now - self.last_heartbeat < HEARTBEAT_EVERY:
            return
        self.last_status = key
        self.last_heartbeat = now
        await asyncio.to_thread(write_status, ok, msg, self.account)

    # ---- حلقه‌ی اصلی ----
    async def tick(self):
        s = await asyncio.to_thread(get_all_settings)
        enabled = s.get("sale_notify_enabled", "1") == "1"
        channels = split_channels(s.get("sale_notify_channel") or DEFAULT_CHANNEL)

        # درخواست تست از پنل
        req = s.get("sale_notify_test_req")
        test_ran = False
        if req and req != s.get("sale_notify_test_done"):
            test_ran = True
            self.hold_until = time.time() + 60
            await asyncio.to_thread(set_setting, "sale_notify_test_done", req)
            if not channels:
                await self.report(False, "برای تست، اول کانال رو تو پنل ذخیره کن.", force=True)
            else:
                ok, failed = await self.do_test(s, channels)
                if failed:
                    bad = " | ".join(f"{c}: {m}" for c, m in failed.items())
                    await self.report(False, f"تست به {len(ok)} از {len(channels)} کانال رسید. ناموفق: {bad}", force=True)
                else:
                    await self.report(True, f"پیام تست به {len(ok)} کانال ارسال شد.", force=True)

        # آخرین سفارشِ دیده‌شده؛ بار اول از «الان» شروع می‌کنیم تا خریدهای قدیمی اسپم نشن
        last_raw = s.get("sale_notify_last_id")
        if last_raw is None or not str(last_raw).lstrip("-").isdigit():
            last_id = await asyncio.to_thread(max_order_id)
            await asyncio.to_thread(set_setting, "sale_notify_last_id", last_id)
        else:
            last_id = int(last_raw)

        orders = await asyncio.to_thread(new_orders, last_id)
        if not orders:
            if not test_ran and time.time() >= self.hold_until:     # نتیجه‌ی تست یک دقیقه تو پنل می‌مونه
                await self.report(True, "فعال و در حال پایش سفارش‌ها" if enabled else "خاموش (از پنل)")
            return

        for order in orders:
            oid, price = order[0], order[5]
            # خاموش یا بدون کانال یا رایگان: فقط جلو می‌ریم تا بعداً یهو اعلان نشه
            if not enabled or not channels or not price or int(price) <= 0:
                last_id = oid
                await asyncio.to_thread(set_setting, "sale_notify_last_id", last_id)
                continue
            ok, failed = await self.send_all(channels, build_message(s, order, True), s)
            if ok:
                # حداقل یه کانال رسید؛ جلو می‌ریم تا تو کانال‌های موفق پیام تکراری نره
                self.fail_count.pop(oid, None)
                last_id = oid
                await asyncio.to_thread(set_setting, "sale_notify_last_id", last_id)
                if failed:
                    bad = " | ".join(f"{c}: {m}" for c, m in failed.items())
                    await self.report(False, f"سفارش #{oid} به {len(ok)} از {len(channels)} کانال رسید. ناموفق: {bad}", force=True)
                else:
                    await self.report(True, f"آخرین اعلان: سفارش #{oid} (به {len(ok)} کانال)", force=True)
            else:
                n = self.fail_count.get(oid, 0) + 1
                self.fail_count[oid] = n
                bad = " | ".join(f"{c}: {m}" for c, m in failed.items())
                print(f"[send] سفارش {oid} به هیچ کانالی نرسید ({n}/{MAX_SEND_RETRIES})")
                await self.report(False, f"ارسال سفارش #{oid} ناموفق بود: {bad}", force=True)
                if n >= MAX_SEND_RETRIES:
                    last_id = oid          # رد می‌شه تا سفارش‌های بعدی گیر نکنن
                    self.fail_count.pop(oid, None)
                    await asyncio.to_thread(set_setting, "sale_notify_last_id", last_id)
                    continue
                break                      # ترتیب حفظ بشه؛ دور بعد دوباره همین سفارش

    async def run(self):
        while ST["gen"] == self.gen:
            try:
                await self.tick()
            except Exception as e:
                print("[tick] error:", repr(e))
                try:
                    await self.report(False, f"خطا: {e!r}")
                except Exception:
                    pass
            await asyncio.sleep(POLL)


# ---------------------------------------------------------------------------
# یوزربات: لاگین از طریق چت ادمین، بعد حلقه‌ی اعلان
# ---------------------------------------------------------------------------
async def userbot_main(phone, gen):
    ST["loop"] = asyncio.get_running_loop()
    client = spluspy.Client(SESSION_NAME)
    kwargs = {"phone": phone}
    try:
        if "code_callback" in inspect.signature(client.start).parameters:
            kwargs["code_callback"] = lambda: ask("code")
    except (TypeError, ValueError):
        pass
    await _maybe_await(client.start(**kwargs))

    cfg = cfg_load()
    cfg["phone"] = phone
    cfg_save(cfg)

    account = None
    try:
        me = await _maybe_await(client.get_me())
        uname = getattr(me, "username", None)
        account = ("@" + uname) if uname else str(getattr(me, "id", ""))
    except Exception as e:
        print("get_me error:", repr(e))

    if ST["gen"] != gen:
        return
    n = Notifier(client, gen, account)
    ST.update(running=True, starting=False, client=client, account=account, notifier=n)
    notify_admin(f"✅ لاگین انجام شد ({account or '—'}). اعلان فروش فعاله.\n"
                 "تست: /saletest  |  وضعیت: /status")
    await asyncio.to_thread(write_status, True, "اکانت وصل شد", account)
    await n.run()


def userbot_thread(phone, gen):
    try:
        asyncio.run(userbot_main(phone, gen))
    except Exception as e:
        print("[userbot] error:", repr(e))
        if ST["gen"] == gen:
            ST.update(running=False, starting=False, waiting=None)
            write_status(False, f"لاگین/اتصال اکانت ناموفق: {e!r}")
            notify_admin(f"❌ خطا در اتصال اکانت:\n{e!r}\n\nدوباره /start بزن.")


def start_userbot(phone):
    ST["gen"] += 1
    gen = ST["gen"]
    ST.update(phone=phone, starting=True, running=False, client=None, notifier=None)
    threading.Thread(target=userbot_thread, args=(phone, gen), daemon=True).start()


async def _safe_disconnect(client):
    await _maybe_await(client.disconnect())


def disconnect_account():
    ST["gen"] += 1                       # حلقه‌ی اعلان قبلی خودش تموم می‌شه
    client, loop = ST["client"], ST["loop"]
    if client is not None and loop is not None:
        try:
            asyncio.run_coroutine_threadsafe(_safe_disconnect(client), loop).result(timeout=10)
        except Exception:
            pass
    try:
        cfg_save({})
    except Exception:
        pass
    for f in glob.glob(SESSION_NAME + "*"):
        try:
            os.remove(f)
        except Exception:
            pass
    ST.update(running=False, starting=False, waiting=None, phone=None, account=None,
              client=None, loop=None, notifier=None)
    write_status(False, "اکانت قطع شد؛ تو چت بات کنترل /start بزن.")


def sale_test_from_chat():
    n, loop = ST.get("notifier"), ST.get("loop")
    if not (ST["running"] and n is not None and loop is not None):
        return False, "اکانت وصل نیست (/start)."
    s = get_all_settings()
    channels = split_channels(s.get("sale_notify_channel") or DEFAULT_CHANNEL)
    if not channels:
        return False, "هنوز کانالی ثبت نشده؛ از پنل (صفحه‌ی «اعلان فروش») یا env ثبتش کن."
    ok, failed = asyncio.run_coroutine_threadsafe(n.do_test(s, channels), loop).result(timeout=90)
    if failed:
        bad = "\n".join(f"• {c}: {m}" for c, m in failed.items())
        return False, (f"به {len(ok)} از {len(channels)} کانال رسید. ناموفق:\n{bad}\n"
                       "مطمئن شو اکانت تو اون کانال‌ها ادمینه و اجازه‌ی ارسال پیام داره.")
    return True, f"پیام تست به {len(ok)} کانال ارسال شد."


# ---------------------------------------------------------------------------
# پیام‌های ادمین
# ---------------------------------------------------------------------------
def handle_update(update):
    m = update.get("message")
    if not m or m.get("chat", {}).get("id") != ADMIN_ID:
        return
    text = (m.get("text") or "").strip()
    contact = m.get("contact")
    cmd = text.split()[0].split("@")[0].lower() if text.startswith("/") else ""

    if spluspy is None:
        if cmd in ("/start", "/status"):
            notify_admin("⚠️ کتابخونه‌ی spluspy روی سرور نصب نیست؛ به requirements اضافه کن و دوباره دیپلوی کن.")
        return

    # جواب کد تأیید / رمز دومرحله‌ای (پیام بعد از دریافت پاک می‌شه)
    if ST["waiting"] and text and not cmd:
        ANSWERS.put(text)
        if m.get("message_id") is not None:
            delete_message(ADMIN_ID, m["message_id"])
        return

    if cmd == "/status":
        status = "🟢 وصل" if ST["running"] else ("🟡 در حال وصل‌شدن" if ST["starting"] else "🔴 وصل نیست (/start)")
        s = get_all_settings()
        chs = split_channels(s.get("sale_notify_channel") or DEFAULT_CHANNEL)
        en = "روشن" if s.get("sale_notify_enabled", "1") == "1" else "خاموش"
        notify_admin(f"وضعیت اکانت: {status}\nاکانت: {ST['account'] or '—'}\nاعلان فروش: {en}\n"
                     "کانال‌ها:\n" + ("\n".join(f"• {c}" for c in chs) or "—"))
        return

    if cmd == "/saletest":
        ok, msg = sale_test_from_chat()
        notify_admin(("✅ " if ok else "❌ ") + msg)
        return

    if cmd == "/disconnect":
        disconnect_account()
        notify_admin("🗑 اکانت قطع شد و سشن پاک شد. برای وصل‌شدن دوباره /start بزن.")
        return

    if cmd == "/start":
        if ST["running"]:
            notify_admin(f"✅ اکانت وصله ({ST['account'] or '—'}). برای قطع کردنش /disconnect بزن.")
            return
        if ST["starting"] or ST["waiting"]:
            notify_admin("⏳ لاگین در حال انجامه. اگه گیر کرده /disconnect بزن و دوباره /start.")
            return
        TSTATE["phone"] = True
        send_message(ADMIN_ID, "📱 شماره‌ی اکانتی رو بفرست که ادمین کانال(های) اعلان فروشه "
                               "(با دکمه‌ی زیر یا دستی تایپ کن).\nبرای رد شدن /cancel بزن.", contact_kb())
        return

    if TSTATE.get("phone"):
        if cmd == "/cancel":
            TSTATE.pop("phone", None)
            send_message(ADMIN_ID, "باشه، لغو شد.", REMOVE_KB)
            return
        if contact or (text and not cmd):
            phone = normalize_phone((contact or {}).get("phone_number") or text)
            if not phone:
                send_message(ADMIN_ID, "این رو نتونستم به‌عنوان شماره بخونم. دوباره بفرست.", contact_kb())
                return
            TSTATE.pop("phone", None)
            print("[login] شماره از ادمین دریافت شد:", phone)
            send_message(ADMIN_ID, f"📲 در حال ورود با شماره {phone} ...", REMOVE_KB)
            start_userbot(phone)
            return
        send_message(ADMIN_ID, "نتونستم شماره رو بخونم. با دکمه‌ی «ارسال شماره من» یا تایپ دستی بفرست "
                               "(برای رد شدن /cancel).", contact_kb())


def poll_loop():
    offset = None
    while True:
        data = get_updates(offset)
        if not data.get("ok"):
            time.sleep(3)
            continue
        for upd in data.get("result", []):
            offset = upd["update_id"] + 1
            try:
                handle_update(upd)
            except Exception as e:
                print("[handle_update] error:", repr(e))


def boot():
    db_init()
    try:
        forget_old_db_session()
    except Exception:
        pass                              # جدول قدیمی وجود نداره؛ مشکلی نیست
    phone = cfg_load().get("phone")
    if spluspy is not None and phone and session_files():
        print("[boot] سشن روی دیسک هست؛ اتصال خودکار.")
        start_userbot(phone)
        return
    # سشنی نیست (اولین اجرا یا بعد از ریدیپلوی): از ادمین بخواه لاگین کنه
    write_status(False, "اکانت وصل نیست؛ تو چت بات کنترل /start بزن.")
    notify_admin("🔐 اکانت اعلان فروش وصل نیست (اولین اجرا یا بات ریدیپلوی شده).\n"
                 "برای وصل‌شدن، /start بزن و شماره رو بفرست، بعد کد رو با خط‌تیره (1-2-3-4-5).")


if __name__ == "__main__":
    try:
        boot()
        poll_loop()
    except KeyboardInterrupt:
        pass
