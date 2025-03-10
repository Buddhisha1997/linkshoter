from flask import Flask, render_template, request, redirect, url_for
import sqlite3
import string
import random
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)
app.config['DATABASE'] = 'database.db'
app.config['SCHEDULER_API_ENABLED'] = False

# ========================
# Database Initialization
# ========================
def init_db():
    with sqlite3.connect(app.config['DATABASE']) as conn:
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS links (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                original_url TEXT NOT NULL,
                short_code TEXT NOT NULL UNIQUE,
                expiry_date DATETIME,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS clicks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                short_code TEXT NOT NULL,
                clicked_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                ip_address TEXT,
                user_agent TEXT,
                FOREIGN KEY(short_code) REFERENCES links(short_code)
            )
        ''')
        conn.commit()

# ========================
# Scheduler Configuration
# ========================
def delete_expired_links():
    with sqlite3.connect(app.config['DATABASE']) as conn:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM links WHERE expiry_date < datetime("now")')
        conn.commit()

scheduler = BackgroundScheduler()
scheduler.add_job(func=delete_expired_links, trigger='interval', hours=1)
scheduler.start()

# ========================
# Context Processors
# ========================
@app.context_processor
def inject_global_stats():
    def get_system_stats():
        with sqlite3.connect(app.config['DATABASE']) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT 
                    COUNT(*) AS total_links,
                    SUM(CASE WHEN expiry_date > datetime('now') THEN 1 ELSE 0 END) AS active_links
                FROM links
            ''')
            link_stats = cursor.fetchone()
            
            cursor.execute('SELECT COUNT(*) AS total_clicks FROM clicks')
            click_stats = cursor.fetchone()
            
            cursor.execute('''
                SELECT l.short_code, COUNT(c.id) AS clicks 
                FROM links l
                LEFT JOIN clicks c ON l.short_code = c.short_code
                GROUP BY l.short_code
                ORDER BY clicks DESC
                LIMIT 5
            ''')
            popular_links = cursor.fetchall()
            
        return {
            'total_links': link_stats['total_links'],
            'active_links': link_stats['active_links'],
            'total_clicks': click_stats['total_clicks'],
            'popular_links': popular_links
        }
    
    return {'system_stats': get_system_stats}

# ========================
# Helper Functions
# ========================
def generate_short_code():
    characters = string.ascii_letters + string.digits
    return ''.join(random.choice(characters) for _ in range(6))

@app.template_filter('datetimeformat')
def datetimeformat(value, format='%Y-%m-%d %H:%M'):
    if not value:
        return "Never"
    return datetime.strptime(value, '%Y-%m-%dT%H:%M').strftime(format)

# ========================
# Application Routes
# ========================
@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        original_url = request.form['url']
        expiry_date = request.form['expiry_date']
        short_code = generate_short_code()

        with sqlite3.connect(app.config['DATABASE']) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO links (original_url, short_code, expiry_date)
                VALUES (?, ?, ?)
            ''', (original_url, short_code, expiry_date))
            conn.commit()

        short_url = request.host_url + short_code
        return render_template('index.html', 
                             short_url=short_url,
                             original_url=original_url)

    return render_template('index.html')

@app.route('/<short_code>')
def redirect_to_original(short_code):
    with sqlite3.connect(app.config['DATABASE']) as conn:
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO clicks (short_code, ip_address, user_agent)
            VALUES (?, ?, ?)
        ''', (short_code, request.remote_addr, request.user_agent.string))
        
        cursor.execute('''
            SELECT original_url, expiry_date 
            FROM links 
            WHERE short_code = ?
        ''', (short_code,))
        result = cursor.fetchone()
        conn.commit()

        if result:
            original_url, expiry_date = result
            if expiry_date and datetime.now() > datetime.strptime(expiry_date, '%Y-%m-%dT%H:%M'):
                return render_template('error.html', 
                                     message="This URL has expired"), 410
            return redirect(original_url)
        return render_template('error.html', 
                             message="URL not found"), 404

@app.route('/all-links')
def all_links():
    with sqlite3.connect(app.config['DATABASE']) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute('''
            SELECT l.*, COUNT(c.id) AS click_count 
            FROM links l
            LEFT JOIN clicks c ON l.short_code = c.short_code
            GROUP BY l.id
            ORDER BY l.created_at DESC
        ''')
        links = cursor.fetchall()
    return render_template('all_links.html', links=links)

@app.route('/click-details/<short_code>')
def click_details(short_code):
    with sqlite3.connect(app.config['DATABASE']) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM links WHERE short_code = ?', (short_code,))
        link = cursor.fetchone()
        
        cursor.execute('''
            SELECT * 
            FROM clicks 
            WHERE short_code = ? 
            ORDER BY clicked_at DESC
        ''', (short_code,))
        clicks = cursor.fetchall()
        
    return render_template('click_details.html', 
                         link=link, 
                         clicks=clicks,
                         total_clicks=len(clicks))

# ========================
# Main Application Runner
# ========================
if __name__ == '__main__':
    init_db()
    try:
        app.run(debug=True)
    finally:
        scheduler.shutdown()