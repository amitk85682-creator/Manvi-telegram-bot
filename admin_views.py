from flask import Blueprint, render_template, request, redirect, url_for, session, flash, current_app
import os
import io
import csv
from werkzeug.utils import secure_filename
import logging
import db_utils

logger = logging.getLogger(__name__)

admin = Blueprint('admin', __name__, template_folder='templates')

ADMIN_WEB_SECRET = os.environ.get('ADMIN_WEB_SECRET', 'change_this_to_secure_value')
ALLOWED_EXTENSIONS = {'csv'}

def login_required(f):
    def wrapper(*args, **kwargs):
        if session.get('logged_in') != True:
            flash('Please login to access admin.', 'error')
            return redirect(url_for('admin.login'))
        return f(*args, **kwargs)
    wrapper.__name__ = f.__name__
    return wrapper

@admin.route('/admin/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        secret = request.form.get('admin_secret', '')
        if secret == ADMIN_WEB_SECRET:
            session['logged_in'] = True
            flash('Login successful.', 'success')
            return redirect(url_for('admin.add_movie_form'))
        else:
            flash('Incorrect secret code.', 'error')
    return render_template('admin_login.html')

@admin.route('/admin/logout')
def logout():
    session.pop('logged_in', None)
    flash('Logged out.', 'success')
    return redirect(url_for('admin.login'))

@admin.route('/admin', methods=['GET'])
@login_required
def add_movie_form():
    return render_template('admin_add_movie.html')

@admin.route('/admin/add_movie', methods=['POST'])
@login_required
def add_movie_post():
    title = request.form.get('title', '').strip()
    description = request.form.get('description', '').strip()
    aliases = request.form.get('aliases', '').strip()

    qualities = {
        '360p': request.form.get('q_360', '').strip(),
        '720p': request.form.get('q_720', '').strip(),
        '1080p': request.form.get('q_1080', '').strip(),
        '2160p': request.form.get('q_2160', '').strip()
    }

    if not title:
        flash('Title is required.', 'error')
        return redirect(url_for('admin.add_movie_form'))

    if not any(qualities.values()):
        flash('At least one quality link/file id is required.', 'error')
        return redirect(url_for('admin.add_movie_form'))

    conn = db_utils.get_db_connection()
    if not conn:
        flash('Database connection failed.', 'error')
        return redirect(url_for('admin.add_movie_form'))

    movie_id = db_utils.upsert_movie_and_files(conn, title, description, qualities, aliases)
    if movie_id:
        flash(f'Movie "{title}" added/updated (id={movie_id}).', 'success')
    else:
        flash(f'Failed to add/update movie "{title}". See logs.', 'error')

    return redirect(url_for('admin.add_movie_form'))

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@admin.route('/admin/bulk_upload', methods=['GET', 'POST'])
@login_required
def bulk_upload():
    if request.method == 'POST':
        if 'csvfile' not in request.files:
            flash('No file part', 'error')
            return redirect(url_for('admin.bulk_upload'))

        file = request.files['csvfile']
        if file.filename == '':
            flash('No selected file', 'error')
            return redirect(url_for('admin.bulk_upload'))

        if file and allowed_file(file.filename):
            try:
                stream = io.StringIO(file.stream.read().decode('utf-8'))
                reader = csv.DictReader(stream)
                conn = db_utils.get_db_connection()
                if not conn:
                    flash('DB connection failed', 'error')
                    return redirect(url_for('admin.bulk_upload'))

                success = 0
                failed = 0
                for row in reader:
                    title = (row.get('Title') or row.get('title') or '').strip()
                    if not title:
                        failed += 1
                        continue
                    description = (row.get('Description') or row.get('description') or '').strip()
                    qualities = {
                        '360p': row.get('URL_360') or row.get('url_360') or row.get('360p') or '',
                        '720p': row.get('URL_720') or row.get('url_720') or row.get('720p') or '',
                        '1080p': row.get('URL_1080') or row.get('url_1080') or row.get('1080p') or '',
                        '2160p': row.get('URL_2160') or row.get('url_2160') or row.get('2160p') or ''
                    }
                    aliases = row.get('Aliases') or row.get('aliases') or ''
                    mid = db_utils.upsert_movie_and_files(conn, title, description, qualities, aliases)
                    if mid:
                        success += 1
                    else:
                        failed += 1

                flash(f'Bulk upload done. Success: {success}, Failed: {failed}', 'success')
                return redirect(url_for('admin.add_movie_form'))
            except Exception as e:
                logger.error(f'Bulk upload error: {e}')
                flash(f'Bulk upload failed: {e}', 'error')
                return redirect(url_for('admin.bulk_upload'))
        else:
            flash('Only CSV files allowed.', 'error')
            return redirect(url_for('admin.bulk_upload'))

    # GET
    return render_template('admin_add_movie.html')
