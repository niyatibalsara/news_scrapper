# from functools import wraps
# from flask import Blueprint, flash, redirect, render_template, request, session, url_for
# from werkzeug.security import check_password_hash, generate_password_hash
# from app.db import get_db

# auth_bp = Blueprint('auth', __name__)


# def login_required(view):
#     @wraps(view)
#     def wrapped_view(*args, **kwargs):
#         if 'user_id' not in session:
#             flash('Please log in to continue.', 'warning')
#             return redirect(url_for('auth.login'))
#         return view(*args, **kwargs)
#     return wrapped_view


# @auth_bp.route('/signup', methods=['GET', 'POST'])
# def signup():
#     if 'user_id' in session:
#         return redirect(url_for('main.home'))

#     if request.method == 'POST':
#         name = request.form.get('name', '').strip()
#         email = request.form.get('email', '').strip().lower()
#         password = request.form.get('password', '')

#         if not name or not email or not password:
#             flash('All fields are required.', 'danger')
#             return render_template('auth/signup.html')

#         if len(password) < 6:
#             flash('Password must be at least 6 characters long.', 'danger')
#             return render_template('auth/signup.html')

#         db = get_db()
#         cursor = db.cursor(dictionary=True)

#         cursor.execute("SELECT id FROM users WHERE email = %s", (email,))
#         existing_user = cursor.fetchone()

#         if existing_user:
#             cursor.close()
#             flash('An account with this email already exists.', 'danger')
#             return render_template('auth/signup.html')

#         hashed_password = generate_password_hash(password)

#         cursor.execute(
#             "INSERT INTO users (name, email, password) VALUES (%s, %s, %s)",
#             (name, email, hashed_password)
#         )
#         db.commit()
#         cursor.close()

#         flash('Account created successfully. Please log in.', 'success')
#         return redirect(url_for('auth.login'))

#     return render_template('auth/signup.html')


# @auth_bp.route('/login', methods=['GET', 'POST'])
# def login():
#     if 'user_id' in session:
#         return redirect(url_for('main.home'))

#     if request.method == 'POST':
#         email = request.form.get('email', '').strip().lower()
#         password = request.form.get('password', '')

#         if not email or not password:
#             flash('Please enter both email and password.', 'danger')
#             return render_template('auth/login.html')

#         db = get_db()
#         cursor = db.cursor(dictionary=True)

#         cursor.execute("SELECT * FROM users WHERE email = %s", (email,))
#         user = cursor.fetchone()
#         cursor.close()

#         if user is None or not check_password_hash(user['password'], password):
#             flash('Invalid email or password.', 'danger')
#             return render_template('auth/login.html')

#         session.clear()
#         session['user_id'] = user['id']
#         session['user_name'] = user['name']
#         session['user_email'] = user['email']

#         flash(f"Welcome back, {user['name']}!", 'success')
#         return redirect(url_for('main.home'))

#     return render_template('auth/login.html')


# @auth_bp.route('/logout')
# @login_required
# def logout():
#     session.clear()
#     flash('You have been logged out successfully.', 'info')
#     return redirect(url_for('auth.login'))

from functools import wraps
from flask import Blueprint, flash, redirect, render_template, request, session, url_for
from werkzeug.security import check_password_hash, generate_password_hash
from app.db import get_db

auth_bp = Blueprint('auth', __name__)


def login_required(view):
    @wraps(view)
    def wrapped_view(*args, **kwargs):
        if 'user_id' not in session:
            flash('Please log in to continue.', 'warning')
            return redirect(url_for('auth.login'))
        return view(*args, **kwargs)
    return wrapped_view


@auth_bp.route('/signup', methods=['GET', 'POST'])
def signup():
    if 'user_id' in session:
        return redirect(url_for('main.home'))

    if request.method == 'POST':
        name = request.form.get('name', '').strip()
        email = request.form.get('email', '').strip().lower()
        password = request.form.get('password', '')

        if not name or not email or not password:
            flash('All fields are required.', 'danger')
            return render_template('auth/signup.html')

        if len(password) < 6:
            flash('Password must be at least 6 characters long.', 'danger')
            return render_template('auth/signup.html')

        db = get_db()
        cursor = db.cursor(dictionary=True)

        cursor.execute("SELECT id FROM users WHERE email = %s", (email,))
        existing_user = cursor.fetchone()

        if existing_user:
            cursor.close()
            flash('An account with this email already exists.', 'danger')
            return render_template('auth/signup.html')

        hashed_password = generate_password_hash(password)

        cursor.execute(
            "INSERT INTO users (name, email, password) VALUES (%s, %s, %s)",
            (name, email, hashed_password)
        )
        db.commit()
        cursor.close()

        flash('Account created successfully. Please log in.', 'success')
        return redirect(url_for('auth.login'))

    return render_template('auth/signup.html')


@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    if 'user_id' in session:
        return redirect(url_for('main.home'))

    if request.method == 'POST':
        email = request.form.get('email', '').strip().lower()
        password = request.form.get('password', '')

        if not email or not password:
            flash('Please enter both email and password.', 'danger')
            return render_template('auth/login.html')

        db = get_db()
        cursor = db.cursor(dictionary=True)

        cursor.execute("SELECT * FROM users WHERE email = %s", (email,))
        user = cursor.fetchone()
        cursor.close()

        if user is None or not check_password_hash(user['password'], password):
            flash('Invalid email or password.', 'danger')
            return render_template('auth/login.html')

        session.clear()
        session['user_id'] = user['id']
        session['user_name'] = user['name']
        session['user_email'] = user['email']

        flash(f"Welcome back, {user['name']}!", 'success')
        return redirect(url_for('main.home'))

    return render_template('auth/login.html')


@auth_bp.route('/logout')
@login_required
def logout():
    session.clear()
    flash('You have been logged out successfully.', 'info')
    return redirect(url_for('main.index'))