"""
بات اعلان فروش SkyTunnel
------------------------
- با اکانت شخصی (پروتکل سروش پلاس، کتابخونه‌ی spluspy) لاگین می‌مونه.
- جدول orders همون دیتابیس Turso بات فروش رو هر چند ثانیه چک می‌کنه.
- برای هر خرید جدید، یه پیام توی کانال می‌فرسته (آیدی خریدار نیمه‌مخفی).
- متن/کانال/روشن‌خاموش از «پنل وب ← اعلان فروش» عوض می‌شه (جدول settings).

به بات فروش اصلی هیچ تغییری لازم نیست.

env های اختیاری (مقدار پیش‌فرض Turso داخل همین فایل ست شده):
  TURSO_DATABASE_URL, TURSO_AUTH_TOKEN   (همون‌هایی که بات فروش داره)
  NOTIFIER_PHONE        شماره‌ی اکانت (برای لاگین اول؛ وگرنه ازت می‌پرسه)
  NOTIFIER_POLL         فاصله‌ی چک‌کردن سفارش‌ها به ثانیه (پیش‌فرض ۵)

لاگین اول: یک‌بار برنامه رو روی سیستم خودت اجرا کن (python sales_notifier.py)، شماره و کد
تأیید رو بزن. سشن داخل دیتابیس ذخیره می‌شه و بعد از اون روی سرور (Railway و...) بدون
لاگین دوباره بالا میاد.
"""
import os
import re
import sys
import json
import glob
import time
import zlib
import base64
import hashlib
import inspect
import asyncio
import sqlite3
import tempfile
import threading
import html as _html
from datetime import datetime, timedelta, timezone

import turso_serverless

try:
    import spluspy
except Exception as _e:  # noqa
    spluspy = None   # فقط حالت اکانت شخصی بهش نیاز داره

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
    raise SystemExit("TURSO_DATABASE_URL و TURSO_AUTH_TOKEN باید به‌صورت env تنظیم بشن.")

SESSION_NAME = "notifier_session"
CONFIG_PATH = "notifier_config.json"
DB_SESSION_KEY = "session"
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
DEFAULT_CHANNEL = os.getenv("NOTIFIER_CHANNEL", "").strip()   # چندتا کانال: با ویرگول یا فاصله جدا کن
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
    c.execute("CREATE TABLE IF NOT EXISTS notifier_storage (name TEXT PRIMARY KEY, value TEXT)")
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


@with_db_retry
def storage_get(name):
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT value FROM notifier_storage WHERE name=?", (name,))
    row = c.fetchone()
    return row[0] if row else None


@with_db_retry
def storage_set(name, value):
    conn = get_conn()
    c = conn.cursor()
    c.execute("INSERT INTO notifier_storage (name, value) VALUES (?, ?) "
              "ON CONFLICT(name) DO UPDATE SET value=excluded.value", (name, value))
    conn.commit()


# ---------------------------------------------------------------------------
# سشن (دیسک ⇄ دیتابیس، تا با ریدیپلوی لاگین از بین نره)
# ---------------------------------------------------------------------------
def _cfg_load():
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _cfg_save(cfg):
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False)


def _session_files():
    skip = ("-journal", "-wal", "-shm")
    return [f for f in glob.glob(SESSION_NAME + "*") if not f.endswith(skip) and os.path.isfile(f)]


def _read_consistent(path):
    try:
        with open(path, "rb") as f:
            head = f.read(16)
        if head.startswith(b"SQLite format 3"):
            fd, tmp = tempfile.mkstemp(suffix=".session")
            os.close(fd)
            try:
                src = sqlite3.connect(path, timeout=5)
                dst = sqlite3.connect(tmp)
                with dst:
                    src.backup(dst)
                src.close()
                dst.close()
                with open(tmp, "rb") as f:
                    return f.read()
            finally:
                try:
                    os.remove(tmp)
                except Exception:
                    pass
    except Exception as e:
        print("[session] backup sqlite failed, raw copy:", repr(e))
    with open(path, "rb") as f:
        return f.read()


_last_hash = {"h": None}


def save_session_to_db(force=False):
    try:
        files = _session_files()
        phone = _cfg_load().get("phone")
        if not files or not phone:
            return False
        payload = {"phone": phone, "files": {
            os.path.basename(f): base64.b64encode(zlib.compress(_read_consistent(f), 6)).decode("ascii")
            for f in files}}
        raw = json.dumps(payload, sort_keys=True)
        h = hashlib.sha256(raw.encode()).hexdigest()
        if not force and _last_hash["h"] == h:
            return True
        storage_set(DB_SESSION_KEY, raw)
        _last_hash["h"] = h
        return True
    except Exception as e:
        print("[session] ذخیره تو دیتابیس ناموفق بود:", repr(e))
        return False


def restore_session_from_db():
    try:
        raw = storage_get(DB_SESSION_KEY)
        if not raw:
            return False
        data = json.loads(raw)
        phone, files = data.get("phone"), data.get("files") or {}
        if not phone or not files:
            return False
        for name, b64 in files.items():
            name = os.path.basename(name)
            if not name.startswith(SESSION_NAME):
                continue
            with open(name, "wb") as f:
                f.write(zlib.decompress(base64.b64decode(b64)))
        cfg = _cfg_load()
        cfg["phone"] = phone
        _cfg_save(cfg)
        print("[session] سشن از دیتابیس بازیابی شد.")
        return True
    except Exception as e:
        print("[session] بازیابی از دیتابیس ناموفق بود:", repr(e))
        return False


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


class Notifier:
    def __init__(self):
        self.client = None
        self.account = None
        self.entities = {}              # channel -> entity
        self.fail_count = {}            # order_id -> تعداد شکست
        self.last_heartbeat = 0.0
        self.last_status = None
        self.hold_until = 0.0            # تا این لحظه نتیجه‌ی تست تو پنل می‌مونه

    # ---- لاگین ----
    async def login(self, phone):
        self.client = spluspy.Client(SESSION_NAME)
        kwargs = {"phone": phone}
        try:
            if "code_callback" in inspect.signature(self.client.start).parameters:
                kwargs["code_callback"] = lambda: re.sub(r"\D", "", input("📩 کد تأیید سروش پلاس: "))
        except (TypeError, ValueError):
            pass
        await _maybe_await(self.client.start(**kwargs))
        cfg = _cfg_load()
        cfg["phone"] = phone
        _cfg_save(cfg)
        await asyncio.to_thread(save_session_to_db, True)
        try:
            me = await _maybe_await(self.client.get_me())
            uname = getattr(me, "username", None)
            self.account = ("@" + uname) if uname else str(getattr(me, "id", ""))
        except Exception as e:
            print("get_me error:", repr(e))
        print("✅ لاگین شد:", self.account or "")

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

    async def send_all(self, channels, text_html, settings):
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

    async def send(self, channel, text_html, settings):
        ent = await self.get_entity(channel)
        try:
            await _maybe_await(self.client.send_message(ent, strip_tags(text_html)))
        except Exception:
            self.entities.pop(channel, None)   # دفعه‌ی بعد دوباره resolve بشه
            raise

    # ---- وضعیت برای پنل ----
    async def report(self, ok, msg, force=False):
        now = time.time()
        key = (ok, msg)
        if not force and key == self.last_status and now - self.last_heartbeat < HEARTBEAT_EVERY:
            return
        self.last_status = key
        self.last_heartbeat = now
        payload = json.dumps({"ts": datetime.now(timezone.utc).isoformat(), "ok": ok,
                              "msg": msg, "account": self.account}, ensure_ascii=False)
        try:
            await asyncio.to_thread(set_setting, "sale_notify_status", payload)
        except Exception as e:
            print("report error:", repr(e))

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
                ok, failed = await self.send_all(
                    channels, "🧪 تست اعلان فروش\n\n" + build_message(s, SAMPLE_ORDER, True), s)
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
        while True:
            try:
                await self.tick()
            except Exception as e:
                print("[tick] error:", repr(e))
                try:
                    await self.report(False, f"خطا: {e!r}")
                except Exception:
                    pass
            await asyncio.sleep(POLL)

    async def session_saver(self):
        while True:
            await asyncio.sleep(300)
            await asyncio.to_thread(save_session_to_db)


async def main():
    await asyncio.to_thread(db_init)
    if spluspy is None:
        raise SystemExit("spluspy نصب نیست: pip install spluspy")

    if not (_cfg_load().get("phone") and _session_files()):
        await asyncio.to_thread(restore_session_from_db)

    phone = _cfg_load().get("phone") or os.getenv("NOTIFIER_PHONE", "").strip()
    if not phone:
        if not sys.stdin or not sys.stdin.isatty():
            raise SystemExit("سشنی تو دیتابیس نیست و ترمینال تعاملی هم نداری. اول یک‌بار روی سیستم خودت "
                             "اجرا کن و لاگین کن؛ بعد روی سرور خودکار بالا میاد.")
        phone = input("📱 شماره‌ی اکانت (مثلاً +98912xxxxxxx): ").strip()
    phone = re.sub(r"[^\d+]", "", phone)
    if not phone.startswith("+"):
        d = re.sub(r"\D", "", phone)
        phone = "+98" + d[1:] if d.startswith("0") else "+" + d

    n = Notifier()
    await n.login(phone)
    await asyncio.gather(n.run(), n.session_saver())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
