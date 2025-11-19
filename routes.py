# LIBRERIE ESTERNE
from flask import render_template, request, redirect, url_for, flash
from model import User
try:
    from icecream import ic
except:
    pass


def register_routes(app):
    @app.login_manager.user_loader
    def load_user(user_id):
        return User.query.get(int(user_id))

    # ====== Routes ======

    # ====== Bootstrap DB alla prima esecuzione ======
    @app.route('/')
    def index():
        return render_template('base.html')

    @app.route('/register', methods=['GET', 'POST'])
    def register():
        if app.current_user.is_authenticated:
            return redirect(url_for('dashboard'))
        if request.method == 'POST':
            name = request.form.get('name', '').strip()
            email = request.form.get('email', '').lower().strip()
            password = request.form.get('password', '')
            if not name or not email or not password:
                flash('Compila tutti i campi.', 'danger')
                return redirect(url_for('register'))
            if User.query.filter_by(email=email).first():
                flash('Email già registrata.', 'warning')
                return redirect(url_for('register'))

            user = User(name=name, email=email)
            user.set_password(password)
            app.db.session.add(user)
            app.db.session.commit()
            flash('Registrazione completata. Ora effettua il login.', 'success')
            return redirect(url_for('login'))
        return render_template('register.html')

    @app.route('/login', methods=['GET', 'POST'])
    def login():
        if app.current_user.is_authenticated:
            return redirect(url_for('dashboard'))
        if request.method == 'POST':
            email = request.form.get('email', '').lower().strip()
            password = request.form.get('password', '')
            remember = bool(request.form.get('remember'))
            user = User.query.filter_by(email=email).first()
            if user and user.check_password(password):
                app.login_user(user, remember=remember)
                flash('Benvenuto!', 'success')
                next_page = request.args.get('next')
                return redirect(next_page or url_for('dashboard'))
            flash('Credenziali non valide.', 'danger')
        return render_template('login.html')

    @app.route('/logout')
    @app.login_required
    def logout():
        app.logout_user()
        flash('Disconnesso.', 'info')
        return redirect(url_for('login'))

    @app.route('/dashboard')
    @app.login_required
    def dashboard():
        return render_template('dashboard.html', user=app.current_user)
