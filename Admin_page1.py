import os
import json
import uuid
import logging
import sys
import operator
import random  # REQUIRED for Ticket Codes
import string  # REQUIRED for Ticket Codes
from flask import (
    Flask,
    render_template,
    request,
    jsonify,
    send_from_directory,
    redirect,
    url_for,
    make_response,
)
from datetime import datetime, timedelta
from werkzeug.utils import secure_filename

app = Flask(__name__, template_folder=".", static_folder=".")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("LBAS_Command_Center")

PROFILE_FOLDER = "Profile"
app.config["UPLOAD_FOLDER"] = PROFILE_FOLDER
app.config["MAX_CONTENT_LENGTH"] = 32 * 1024 * 1024
app.config["JSONIFY_PRETTYPRINT_REGULAR"] = True

if not os.path.exists(PROFILE_FOLDER):
    os.makedirs(PROFILE_FOLDER)
    logger.info(f"SYSTEM INIT: Created secure profile storage at ./{PROFILE_FOLDER}")

# Database Map: Full restoration of all required DBs
DB_FILES = {
    "books": "books.json",
    "admins": "admins.json",
    "users": "users.json",
    "transactions": "transactions.json",
    "ratings": "ratings.json",
    "config": "system_config.json",
    "tickets": "tickets.json",  # Password Recovery Registry
    "categories": "categories.json",
}

ACTIVE_SESSIONS = {}


def initialize_system():
    logger.info("SYSTEM INIT: verifying database integrity...")
    for key, file_path in DB_FILES.items():
        if not os.path.exists(file_path):
            if key == "config":
                initial_data = {
                    "system_version": "4.8.3",
                    "rating_enabled": True,
                    "last_reboot": datetime.now().strftime("%Y-%m-%d %H:%M"),
                }
            elif key == "categories":
                initial_data = ["General", "Mathematics", "Science", "Literature"]
            else:
                initial_data = []
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(initial_data, f, indent=4)

    # Ensure categories are available and in sync with book data
    sync_categories_with_books()

    # MIGRATION: Ensure status fields exist
    users = get_db("users")
    changed = False
    for u in users:
        if "status" not in u:
            u["status"] = "approved"
            changed = True
    if changed:
        save_db("users", users)

    # Ensure Root Admin exists
    admins = get_db("admins")
    if not admins:
        admins.append(
            {
                "name": "System Administrator",
                "school_id": "admin",
                "password": "admin",
                "category": "Staff",
                "photo": "default.png",
                "status": "approved",
                "created_at": "SYSTEM_INIT",
            }
        )
        save_db("admins", admins)


def get_db(key):
    try:
        if not os.path.exists(DB_FILES[key]):
            return {} if key == "config" else []
        with open(DB_FILES[key], "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"DB READ ERROR ({key}): {e}")
        return {} if key == "config" else []


def save_db(key, data):
    try:
        with open(DB_FILES[key], "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
    except Exception as e:
        logger.error(f"DB WRITE ERROR ({key}): {e}")


def sanitize_category_name(value):
    clean = str(value or "").strip()
    return clean[:80] if clean else ""


def get_categories():
    categories = get_db("categories")
    if not isinstance(categories, list):
        categories = []

    clean = []
    for c in categories:
        normalized = sanitize_category_name(c)
        if normalized and normalized not in clean:
            clean.append(normalized)

    for default in ["General", "Mathematics", "Science", "Literature"]:
        if default not in clean:
            clean.append(default)
    return clean


def save_categories(categories):
    unique = []
    for c in categories:
        normalized = sanitize_category_name(c)
        if normalized and normalized not in unique:
            unique.append(normalized)
    save_db("categories", unique)
    return unique


def sync_categories_with_books():
    categories = get_categories()
    for b in get_db("books"):
        cat = sanitize_category_name(b.get("category"))
        if cat and cat not in categories:
            categories.append(cat)
    return save_categories(categories)


def find_any_user(s_id):
    s_id = str(s_id).strip().lower()
    if not s_id:
        return None

    for admin in get_db("admins"):
        if str(admin.get("school_id", "")).strip().lower() == s_id:
            admin["registry_origin"] = "admins.json"
            admin["is_staff"] = True
            return admin

    for student in get_db("users"):
        if str(student.get("school_id", "")).strip().lower() == s_id:
            student["registry_origin"] = "users.json"
            student["is_staff"] = False
            return student
    return None


def is_mobile_request():
    ua = request.headers.get("User-Agent", "").lower()
    return any(
        x in ua for x in ["mobile", "android", "iphone", "ipad", "windows phone"]
    )


def run_auto_sync_engine():
    """
    CRITICAL SYNC ENGINE (RESTORED):
    1. Manages Book Reservations (Expires them after 30 mins).
    2. Manages Ticket Requests (Deletes them after 5 mins).
    3. Manages Overdue Calculations.
    """
    books = get_db("books")
    transactions = get_db("transactions")
    tickets = get_db("tickets")
    now = datetime.now()
    changes_made = False

    # 1. Sync Reservations (Expire if not claimed)
    for t in transactions:
        if t["status"] == "Reserved" and "expiry" in t:
            try:
                if now > datetime.strptime(t["expiry"], "%Y-%m-%d %H:%M"):
                    t["status"] = "Expired"
                    for b in books:
                        if b["book_no"] == t["book_no"]:
                            b["status"] = "Available"
                            changes_made = True
            except:
                pass

    # 2. Sync Recovery Tickets (Cleanup expired)
    initial_tickets = len(tickets)
    tickets = [
        t for t in tickets if datetime.strptime(t["expiry"], "%Y-%m-%d %H:%M:%S") > now
    ]
    if len(tickets) != initial_tickets:
        save_db("tickets", tickets)

    if changes_made:
        save_db("books", books)
        save_db("transactions", transactions)

    return books


@app.route("/")
def index_gateway():
    if is_mobile_request():
        return redirect(url_for("lbas_site"))
    # Pre-load data for dashboard
    return render_template(
        "admin_dashboard.html",
        books=run_auto_sync_engine(),
        users=get_db("users"),
        admins=get_db("admins"),
    )


@app.route("/lbas")
def lbas_site():
    return render_template("LBAS.html")


@app.route("/tablet")
def tablet_kiosk():
    """Restored Kiosk Mode for Library Tablet"""
    return render_template("user_tablet.html")


@app.route("/audit_users")
def audit_view():
    return render_template("Admin_users_list.html")


@app.route("/dev/analysis")
def dev_analysis():
    return render_template("Developers_rate_analysis.html")


@app.route("/api/bulk_register", methods=["POST"])
def bulk_register():
    """
    SMART BULK IMPORTER:
    Handles '|', ',', or Space delimiters.
    Fixes the issue where 'LIT-001, Title' was failing.
    """
    try:
        data = request.json
        raw_text = data.get("text", "")
        category = sanitize_category_name(data.get("category", "General")) or "General"
        clear_first = data.get("clear_first", False)

        books = [] if clear_first else get_db("books")
        added = 0

        for line in raw_text.strip().split("\n"):
            line = line.strip()
            if not line:
                continue

            # DELIMITER DETECTION
            if "|" in line:
                parts = [p.strip() for p in line.split("|")]
            elif "," in line:
                parts = [p.strip() for p in line.split(",", 1)]
            else:
                parts = line.split(maxsplit=1)

            if len(parts) >= 2:
                b_no = parts[0].strip().upper().replace(",", "")  # Clean ID
                title = parts[1].strip()

                # Duplicate Check
                if not any(b["book_no"] == b_no for b in books):
                    books.append(
                        {
                            "book_no": b_no,
                            "title": title,
                            "status": "Available",
                            "category": category,
                        }
                    )
                    added += 1

        save_db("books", books)
        categories = sync_categories_with_books()
        # Return keys for both legacy and new frontend versions
        return jsonify(
            {
                "success": True,
                "added": added,
                "items_added": added,
                "total_in_db": len(books),
                "categories": categories,
            }
        )
    except Exception as e:
        logger.error(f"Bulk Import Failed: {e}")
        return jsonify({"success": False, "message": str(e)}), 500


@app.route("/api/request_reset", methods=["POST"])
def api_request_reset():
    """Step 1: Student requests ticket."""
    s_id = str(request.json.get("school_id", "")).strip().lower()
    if not find_any_user(s_id):
        return jsonify({"success": False, "message": "ID not found"}), 404

    tickets = get_db("tickets")
    tickets = [t for t in tickets if t["school_id"] != s_id]  # Clean old requests

    tickets.append(
        {
            "school_id": s_id,
            "status": "pending",
            "code": None,
            "expiry": (datetime.now() + timedelta(minutes=5)).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
        }
    )
    save_db("tickets", tickets)
    return jsonify({"success": True})


@app.route("/api/check_ticket_status", methods=["POST"])
def api_check_ticket():
    """Step 2: Mobile checks if approved."""
    s_id = str(request.json.get("school_id", "")).strip().lower()
    tickets = get_db("tickets")
    ticket = next((t for t in tickets if t["school_id"] == s_id), None)

    if ticket and ticket["status"] == "approved":
        return jsonify({"status": "approved", "code": ticket["code"]})
    return jsonify({"status": "pending"})


@app.route("/api/admin/tickets")
def api_get_tickets():
    """Step 3: Dashboard gets list."""
    return jsonify(get_db("tickets"))


@app.route("/api/admin/approve_ticket", methods=["POST"])
def api_approve_ticket():
    """Step 4: Admin approves & generates code."""
    s_id = request.json.get("school_id")
    tickets = get_db("tickets")
    for t in tickets:
        if t["school_id"] == s_id:
            t["status"] = "approved"
            t["code"] = "".join(
                random.choices(string.ascii_uppercase + string.digits, k=6)
            )
            save_db("tickets", tickets)
            return jsonify({"success": True, "code": t["code"]})
    return jsonify({"success": False}), 404


@app.route("/api/finalize_reset", methods=["POST"])
def api_finalize_reset():
    """Step 5: Apply new password."""
    data = request.json
    s_id = str(data.get("school_id", "")).strip().lower()
    new_pwd = data.get("new_password")
    code = data.get("code")

    tickets = get_db("tickets")
    ticket = next(
        (t for t in tickets if t["school_id"] == s_id and t["code"] == code), None
    )

    if ticket:
        # Update user registry
        for db in ["users", "admins"]:
            registry = get_db(db)
            updated = False
            for u in registry:
                if u["school_id"] == s_id:
                    u["password"] = new_pwd
                    updated = True
            if updated:
                save_db(db, registry)

        # Consume ticket
        save_db("tickets", [t for t in tickets if t["school_id"] != s_id])
        return jsonify({"success": True})
    return jsonify({"success": False}), 401


@app.route("/api/register_student", methods=["POST"])
def api_reg_student():
    try:
        name = request.form.get("name")
        school_id = request.form.get("school_id")
        password = request.form.get("password")
        photo = request.files.get("photo")

        s_id = str(school_id or "").strip().lower()
        if not name or not s_id or not password:
            return jsonify({"success": False, "message": "Missing required fields"}), 400

        if find_any_user(s_id):
            return jsonify({"success": False, "message": "ID Exists"}), 400

        saved_photo = "default.png"
        if photo and photo.filename:
            _, ext = os.path.splitext(photo.filename)
            ext = ext.lower()[:10] if ext else ".png"
            filename = secure_filename(f"{s_id}_{int(datetime.now().timestamp())}{ext}")
            if filename:
                photo.save(os.path.join(app.config["UPLOAD_FOLDER"], filename))
                saved_photo = filename

        users = get_db("users")
        users.append(
            {
                "name": name,
                "school_id": s_id,
                "password": password,
                "category": "Student",
                "photo": saved_photo,
                "status": "pending",
                "created_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
            }
        )
        save_db("users", users)
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)})


@app.route("/api/register_librarian", methods=["POST"])
def api_reg_staff():
    return perform_registration("admins", "Staff")


def perform_registration(target_db_key, category_name):
    if request.is_json:
        data = request.json
        name = data.get("name")
        s_id = str(data.get("school_id")).strip().lower()
        pwd = data.get("password")
    else:
        name = request.form.get("name")
        s_id = str(request.form.get("school_id")).strip().lower()
        pwd = request.form.get("password")

    if find_any_user(s_id):
        return jsonify({"success": False, "message": "ID Exists"}), 400

    photo = "default.png"
    if "photo" in request.files:
        f = request.files["photo"]
        if f.filename != "":
            ext = f.filename.split(".")[-1]
            photo = secure_filename(f"{s_id}_{int(datetime.now().timestamp())}.{ext}")
            f.save(os.path.join(app.config["UPLOAD_FOLDER"], photo))

    # Students = Pending, Staff = Approved
    status = "approved" if category_name == "Staff" else "pending"

    registry = get_db(target_db_key)
    registry.append(
        {
            "name": name,
            "school_id": s_id,
            "password": pwd,
            "category": category_name,
            "photo": photo,
            "status": status,
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        }
    )
    save_db(target_db_key, registry)
    return jsonify({"success": True})


@app.route("/api/login", methods=["POST"])
def api_login():
    data = request.json
    s_id = str(data.get("school_id", "")).strip().lower()
    pwd = data.get("password")

    user = find_any_user(s_id)
    if not user:
        return jsonify({"success": False, "message": "ID not found"}), 404

    if user["status"] == "pending":
        return jsonify({"success": False, "message": "Account Pending Approval"}), 401

    if user.get("password") == pwd:
        token = str(uuid.uuid4())
        ACTIVE_SESSIONS[s_id] = token
        return jsonify({"success": True, "token": token, "profile": user})

    return jsonify({"success": False, "message": "Invalid Password"}), 401


@app.route("/api/books")
def api_get_books():
    return jsonify(run_auto_sync_engine())


@app.route("/api/categories")
def api_get_categories():
    return jsonify(sync_categories_with_books())


@app.route("/api/categories", methods=["POST"])
def api_add_category():
    category = sanitize_category_name(request.json.get("category"))
    if not category:
        return jsonify({"success": False, "message": "Invalid category name"}), 400

    categories = get_categories()
    if category in categories:
        return jsonify({"success": True, "categories": categories, "created": False})

    categories.append(category)
    categories = save_categories(categories)
    return jsonify({"success": True, "categories": categories, "created": True})


@app.route("/api/categories/delete", methods=["POST"])
def api_delete_category():
    category = sanitize_category_name(request.json.get("category"))
    if not category:
        return jsonify({"success": False, "message": "Invalid category name"}), 400

    books_using = [
        b
        for b in get_db("books")
        if sanitize_category_name(b.get("category")) == category
    ]
    if books_using:
        return (
            jsonify(
                {"success": False, "message": "Category is in use by existing books"}
            ),
            400,
        )

    categories = [c for c in get_categories() if c != category]
    save_categories(categories)
    return jsonify({"success": True, "categories": categories})


@app.route("/api/delete_category", methods=["POST"])
def api_delete_category_cascade():
    books_snapshot = get_db("books")
    transactions_snapshot = get_db("transactions")
    categories_snapshot = get_categories()

    category = sanitize_category_name((request.json or {}).get("category"))
    if not category or category == "All Collections":
        return jsonify({"success": False, "message": "Invalid category name"}), 400

    try:
        books_to_delete = {
            b.get("book_no")
            for b in books_snapshot
            if sanitize_category_name(b.get("category")) == category
        }

        filtered_transactions = [
            t for t in transactions_snapshot if t.get("book_no") not in books_to_delete
        ]
        filtered_books = [
            b
            for b in books_snapshot
            if sanitize_category_name(b.get("category")) != category
        ]

        save_db("transactions", filtered_transactions)
        save_db("books", filtered_books)
        save_categories([c for c in categories_snapshot if c != category])

        return jsonify({"success": True})
    except Exception as e:
        logger.error(f"DELETE CATEGORY ERROR: {e}")
        save_db("transactions", transactions_snapshot)
        save_db("books", books_snapshot)
        save_categories(categories_snapshot)
        return jsonify({"success": False}), 500


@app.route("/api/users")
def api_get_users():
    return jsonify(get_db("users"))


@app.route("/api/admins")
def api_get_admins():
    return jsonify(get_db("admins"))


@app.route("/api/transactions")
def api_get_transactions():
    return jsonify(get_db("transactions"))


@app.route("/api/user/<s_id>")
def api_get_specific_user(s_id):
    """Restored: Required for Tablet Kiosk to Scan User"""
    user = find_any_user(s_id)
    if user:
        return jsonify({"success": True, "profile": user})
    return jsonify({"success": False}), 404


@app.route("/api/update_book", methods=["POST"])
def api_update_book():
    data = request.json
    books = get_db("books")
    for b in books:
        if b["book_no"] == data["book_no"]:
            if "category" in data:
                data["category"] = sanitize_category_name(data["category"]) or "General"
            b.update({k: v for k, v in data.items() if k in b})
            save_db("books", books)
            sync_categories_with_books()
            return jsonify({"success": True})
    return jsonify({"success": False}), 404


@app.route("/api/delete_book", methods=["POST"])
def api_del_book():
    data = request.json
    books = [b for b in get_db("books") if b["book_no"] != data["book_no"]]
    save_db("books", books)
    sync_categories_with_books()
    return jsonify({"success": True})


@app.route("/api/update_member", methods=["POST"])
def api_update_member():
    data = request.json
    school_id = str(data.get("school_id", "")).strip().lower()
    name = str(data.get("name", "")).strip()
    target_type = str(data.get("type", "student")).strip().lower()

    if not school_id or not name:
        return jsonify({"success": False, "message": "Missing required fields"}), 400

    db_key = "admins" if target_type == "admin" else "users"
    records = get_db(db_key)
    for row in records:
        if str(row.get("school_id", "")).strip().lower() == school_id:
            row["name"] = name
            save_db(db_key, records)
            return jsonify({"success": True})
    return jsonify({"success": False, "message": "Member not found"}), 404


@app.route("/api/delete_member", methods=["POST"])
def api_delete_member():
    data = request.json
    school_id = str(data.get("school_id", "")).strip().lower()
    target_type = str(data.get("type", "student")).strip().lower()

    if not school_id:
        return jsonify({"success": False, "message": "Missing school_id"}), 400

    db_key = "admins" if target_type == "admin" else "users"
    records = get_db(db_key)
    filtered = [
        r for r in records if str(r.get("school_id", "")).strip().lower() != school_id
    ]
    if len(filtered) == len(records):
        return jsonify({"success": False, "message": "Member not found"}), 404

    save_db(db_key, filtered)
    return jsonify({"success": True})


@app.route("/api/approve_user", methods=["POST"])
def api_approve_user():
    data = request.json
    users = get_db("users")
    for u in users:
        if u["school_id"] == data["school_id"]:
            u["status"] = "approved"
            save_db("users", users)
            return jsonify({"success": True})
    return jsonify({"success": False}), 404


@app.route("/api/reject_user", methods=["POST"])
def api_reject_user():
    data = request.json
    users = [u for u in get_db("users") if u["school_id"] != data["school_id"]]
    save_db("users", users)
    return jsonify({"success": True})


@app.route("/api/process_transaction", methods=["POST"])
def api_process_trans():
    """
    MASTER TRANSACTION HANDLER
    Restored: Now handles 'borrow' logic for Kiosk/Tablet.
    """
    data = request.json
    b_no = data.get("book_no")
    action = data.get("action")  # 'borrow' or 'return'
    s_id = str(data.get("school_id", "")).strip().lower()

    books = get_db("books")
    transactions = get_db("transactions")

    # LOGIC 1: RETURN
    if action == "return":
        for b in books:
            if b["book_no"] == b_no:
                b["status"] = "Available"
        # Close all open transactions for this book
        for t in transactions:
            if t["book_no"] == b_no and t["status"] in ["Reserved", "Borrowed"]:
                t["status"] = "Returned"
                t["return_date"] = datetime.now().strftime("%Y-%m-%d %H:%M")

    # LOGIC 2: BORROW (Restored for Tablet)
    elif action == "borrow":
        target_book = next((b for b in books if b["book_no"] == b_no), None)

        # Validation: Is it available or reserved by THIS user?
        user_reserved = any(
            t["book_no"] == b_no
            and t["school_id"] == s_id
            and t["status"] == "Reserved"
            for t in transactions
        )

        if target_book and (target_book["status"] == "Available" or user_reserved):
            target_book["status"] = "Borrowed"

            # Close reservation if exists
            for t in transactions:
                if t["book_no"] == b_no and t["status"] == "Reserved":
                    t["status"] = "Converted"  # Mark old reservation as done

            # Create Borrow Record
            transactions.append(
                {
                    "book_no": b_no,
                    "school_id": s_id,
                    "status": "Borrowed",
                    "date": datetime.now().strftime("%Y-%m-%d %H:%M"),
                    "expiry": (datetime.now() + timedelta(days=7)).strftime(
                        "%Y-%m-%d %H:%M"
                    ),  # 7 Day Loan
                }
            )
        else:
            return jsonify({"success": False, "message": "Book Unavailable"}), 400

    save_db("books", books)
    save_db("transactions", transactions)
    return jsonify({"success": True})


@app.route("/api/reserve", methods=["POST"])
def api_reserve():
    data = request.json
    b_no = data.get("book_no")
    s_id = str(data.get("school_id", "")).strip().lower()

    books = get_db("books")
    transactions = get_db("transactions")
    now = datetime.now()

    # 1) Cleanup expired reservations for this user before any validation.
    expired_found = False
    for t in transactions:
        if t.get("school_id") != s_id or t.get("status") != "Reserved":
            continue
        expiry_raw = t.get("expiry")
        if not expiry_raw:
            continue
        try:
            if now > datetime.strptime(expiry_raw, "%Y-%m-%d %H:%M"):
                t["status"] = "Expired"
                expired_found = True
                for b in books:
                    if b.get("book_no") == t.get("book_no") and b.get("status") == "Reserved":
                        b["status"] = "Available"
                        break
        except ValueError:
            continue

    # 2) Query active reservations after cleanup.
    active_reservations = [
        t
        for t in transactions
        if t.get("school_id") == s_id and t.get("status") == "Reserved"
    ]

    # 3) Block duplicate active reservation for same book.
    if any(t.get("book_no") == b_no for t in active_reservations):
        if expired_found:
            save_db("books", books)
            save_db("transactions", transactions)
        return (
            jsonify(
                {
                    "success": False,
                    "status": "error",
                    "message": "You already have an active reservation for this book.",
                }
            ),
            400,
        )

    # 4) Enforce max active reservation count.
    if len(active_reservations) >= 5:
        if expired_found:
            save_db("books", books)
            save_db("transactions", transactions)
        return (
            jsonify(
                {
                    "success": False,
                    "status": "error",
                    "message": "Reservation limit reached (5 max).",
                }
            ),
            400,
        )

    for b in books:
        if b["book_no"] == b_no and b["status"] == "Available":
            b["status"] = "Reserved"
            transactions.append(
                {
                    "book_no": b_no,
                    "school_id": s_id,
                    "status": "Reserved",
                    "date": now.strftime("%Y-%m-%d %H:%M"),
                    "expiry": (now + timedelta(minutes=30)).strftime(
                        "%Y-%m-%d %H:%M"
                    ),
                }
            )
            save_db("books", books)
            save_db("transactions", transactions)
            return jsonify({"success": True})

    if expired_found:
        save_db("books", books)
        save_db("transactions", transactions)

    return jsonify({"success": False, "message": "Unavailable"})


@app.route("/dev/analysis")
def dev_analysis_portal():
    """Admin-only portal for rating metrics and database health."""
    if is_mobile_request():
        return "Access Forbidden: Desktop Analysis only.", 403
    return render_template("Developers_rate_analysis.html")


@app.route("/api/toggle_rating", methods=["POST"])
def api_toggle_rating():
    """Global switch to enable/disable the rating prompt on Tablet/LBAS."""
    config = get_db("config")
    current = config.get("rating_enabled", False)
    config["rating_enabled"] = not current
    config["last_modified"] = datetime.now().strftime("%Y-%m-%d %H:%M")
    save_db("config", config)
    return jsonify({"success": True, "new_state": config["rating_enabled"]})


@app.route("/api/rating_status/<school_id>")
def api_rating_eligibility(school_id):
    """Checks if a user has already rated to prevent spam."""
    config = get_db("config")
    if not config.get("rating_enabled", False):
        return jsonify({"show": False, "reason": "System Closed"})

    ratings = get_db("ratings")
    search_id = str(school_id).strip().lower()
    already_done = any(
        str(r.get("school_id")).strip().lower() == search_id for r in ratings
    )
    return jsonify({"show": not already_done})


@app.route("/api/rate", methods=["POST"])
def api_submit_rating():
    """Saves student feedback with session token validation."""
    data = request.json
    s_id = str(data.get("school_id", "")).strip().lower()

    if ACTIVE_SESSIONS.get(s_id) != data.get("token"):
        return jsonify({"success": False, "message": "Security Handshake Failed"}), 401

    ratings = get_db("ratings")
    ratings.append(
        {
            "rating_id": str(uuid.uuid4())[:10],
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "school_id": s_id,
            "stars": int(data.get("stars", 5)),
            "feedback": data.get("feedback", "N/A"),
            "platform": "Mobile" if is_mobile_request() else "Tablet",
        }
    )
    save_db("ratings", ratings)
    return jsonify({"success": True})


@app.route("/api/ratings_summary")
def api_get_ratings():
    """Data feed for the Developer Analysis dashboard."""
    return jsonify(get_db("ratings"))


import sqlite3
from collections import Counter


def _build_leaderboard_db():
    """Builds an in-memory SQL table from JSON transactions for monthly leaderboard queries."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE transactions (book_no TEXT, school_id TEXT, status TEXT, date TEXT)"
    )

    for t in get_db("transactions"):
        conn.execute(
            "INSERT INTO transactions (book_no, school_id, status, date) VALUES (?, ?, ?, ?)",
            (
                str(t.get("book_no", "")).strip(),
                str(t.get("school_id", "")).strip(),
                str(t.get("status", "")).strip(),
                str(t.get("date", "")).strip(),
            ),
        )
    conn.commit()
    return conn


def _parse_transaction_date(raw_date):
    value = str(raw_date or "").strip()
    if not value:
        return None
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def _extract_transaction_date(tx):
    """Supports legacy and new date keys used by transaction records."""
    return _parse_transaction_date(tx.get("transaction_date") or tx.get("date"))


def _current_month_borrowed_transactions():
    now = datetime.now()
    valid_rows = []
    for tx in get_db("transactions"):
        tx_date = _extract_transaction_date(tx)
        if not tx_date:
            continue
        if tx_date.year == now.year and tx_date.month == now.month:
            if str(tx.get("status", "")).strip().lower() in {"borrowed", "returned"}:
                valid_rows.append(tx)
    return valid_rows


def _build_monthly_leaderboard_payload(limit=10):
    monthly_transactions = _current_month_borrowed_transactions()
    books_map = {
        str(b.get("book_no", "")).strip().lower(): b for b in get_db("books")
    }
    profile_map = {}
    for user in get_db("users") + get_db("admins"):
        sid = str(user.get("school_id", "")).strip().lower()
        if sid and sid not in profile_map:
            profile_map[sid] = user

    borrower_counter = Counter()
    borrower_books = {}

    for tx in monthly_transactions:
        sid = str(tx.get("school_id", "")).strip()
        book_no = str(tx.get("book_no", "")).strip()
        if not sid or not book_no:
            continue

        borrower_counter[sid] += 1
        borrower_books.setdefault(sid, []).append(book_no)

    sorted_borrowers = sorted(
        borrower_counter.items(), key=lambda item: (-item[1], str(item[0]).lower())
    )[:limit]
    top_borrowers = []
    for idx, (sid, total) in enumerate(sorted_borrowers, start=1):
        profile = profile_map.get(str(sid).lower(), {})
        books_this_month = borrower_books.get(sid, [])
        favorite_book_no = ""
        favorite_book_title = "No records"
        if books_this_month:
            favorite_book_no, _ = Counter(books_this_month).most_common(1)[0]
            book_match = books_map.get(favorite_book_no.lower(), {})
            favorite_book_title = book_match.get("title") or favorite_book_no

        top_borrowers.append(
            {
                "rank": idx,
                "school_id": sid,
                "name": profile.get("name") or sid,
                "photo": profile.get("photo") or "default.png",
                "total_borrowed": total,
                "most_borrowed_book": f"{favorite_book_no} {favorite_book_title}".strip(),
            }
        )

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE monthly_transactions (book_no TEXT, status TEXT, transaction_date TEXT)"
    )
    conn.execute("CREATE TABLE books (book_no TEXT, title TEXT)")

    for tx in monthly_transactions:
        tx_date = _extract_transaction_date(tx)
        if not tx_date:
            continue
        conn.execute(
            "INSERT INTO monthly_transactions (book_no, status, transaction_date) VALUES (?, ?, ?)",
            (
                str(tx.get("book_no", "")).strip(),
                str(tx.get("status", "")).strip().lower(),
                tx_date.strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )

    for book in get_db("books"):
        conn.execute(
            "INSERT INTO books (book_no, title) VALUES (?, ?)",
            (
                str(book.get("book_no", "")).strip(),
                str(book.get("title", "")).strip(),
            ),
        )

    now = datetime.now()
    rows = conn.execute(
        """
        SELECT
            mt.book_no,
            COALESCE(NULLIF(b.title, ''), mt.book_no) AS title,
            COUNT(*) AS total_borrowed
        FROM monthly_transactions mt
        LEFT JOIN books b ON LOWER(b.book_no) = LOWER(mt.book_no)
        WHERE
            mt.book_no IS NOT NULL
            AND TRIM(mt.book_no) != ''
            AND mt.status IN ('borrowed', 'returned')
            AND CAST(strftime('%m', mt.transaction_date) AS INTEGER) = ?
            AND CAST(strftime('%Y', mt.transaction_date) AS INTEGER) = ?
        GROUP BY mt.book_no, title
        ORDER BY total_borrowed DESC, LOWER(mt.book_no) ASC
        LIMIT ?
        """,
        (now.month, now.year, int(limit)),
    ).fetchall()

    top_books = [
        {
            "rank": idx,
            "book_no": row["book_no"],
            "title": row["title"],
            "total_borrowed": row["total_borrowed"],
        }
        for idx, row in enumerate(rows, start=1)
    ]

    conn.close()

    return {"top_borrowers": top_borrowers, "top_books": top_books}


def _is_staff_session_valid():
    """Checks active staff session for protected leaderboard APIs."""
    staff_id = (
        str(request.headers.get("X-School-Id", request.args.get("school_id", "")))
        .strip()
        .lower()
    )
    token = str(
        request.headers.get("X-Session-Token", request.args.get("token", ""))
    ).strip()
    if not staff_id or ACTIVE_SESSIONS.get(staff_id) != token:
        return False
    user = find_any_user(staff_id)
    return bool(user and user.get("is_staff"))


@app.route("/api/leaderboard/top-borrowers")
def api_leaderboard_top_borrowers():
    """Top 10 borrowers for the current month (public endpoint)."""
    payload = _build_monthly_leaderboard_payload(limit=10)
    return jsonify(
        [
            {
                "school_id": row["school_id"],
                "total": row["total_borrowed"],
                "name": row["name"],
                "photo": row["photo"],
            }
            for row in payload["top_borrowers"]
        ]
    )


@app.route("/api/leaderboard/top-books")
def api_leaderboard_top_books():
    """Top 10 books for the current month (staff only endpoint)."""
    if not _is_staff_session_valid():
        return jsonify({"success": False, "message": "Unauthorized"}), 403

    payload = _build_monthly_leaderboard_payload(limit=10)
    return jsonify(
        [
            {"book_no": row["book_no"], "total": row["total_borrowed"]}
            for row in payload["top_books"]
        ]
    )


@app.route("/api/monthly_leaderboard")
def api_monthly_leaderboard():
    return jsonify(_build_monthly_leaderboard_payload(limit=10))


@app.route("/api/leaderboard_profile/<school_id>")
def api_leaderboard_profile(school_id):
    lookup_id = str(school_id or "").strip()
    if not lookup_id:
        return jsonify({"success": False, "message": "Missing school_id"}), 400

    leaderboard = _build_monthly_leaderboard_payload(limit=1000)
    match = next(
        (
            row
            for row in leaderboard["top_borrowers"]
            if str(row.get("school_id", "")).lower() == lookup_id.lower()
        ),
        None,
    )

    if not match:
        user = find_any_user(lookup_id)
        if not user:
            return jsonify({"success": False, "message": "Profile not found"}), 404
        match = {
            "school_id": user.get("school_id") or lookup_id,
            "name": user.get("name") or lookup_id,
            "photo": user.get("photo") or "default.png",
            "total_borrowed": 0,
            "most_borrowed_book": "No records",
        }

    return jsonify({"success": True, "profile": match})


@app.route("/Profile/<path:filename>")
def serve_file(filename):
    try:
        return send_from_directory(app.config["UPLOAD_FOLDER"], filename)
    except:
        return send_from_directory(app.config["UPLOAD_FOLDER"], "default.png")


if __name__ == "__main__":
    initialize_system()
    app.run(host="0.0.0.0", port=80, debug=True)
