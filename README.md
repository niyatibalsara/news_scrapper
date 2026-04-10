# Seamless News Scraper - Flask Frontend Starter

This project includes:
- Professional landing page with company logo
- Sign Up and Log In pages
- SQLite-based authentication flow
- Password hashing for secure credential storage
- Protected homepage after login
- Navbar with View Master, News Processing, and Logout
- Clean Flask project structure for future expansion

## Project Structure

```text
news_scraper_flask/
├── app/
│   ├── __init__.py
│   ├── db.py
│   ├── routes/
│   │   ├── auth.py
│   │   └── main.py
│   ├── static/
│   │   ├── css/style.css
│   │   ├── images/seamless-logo.jfif
│   │   └── js/main.js
│   └── templates/
│       ├── auth/
│       ├── main/
│       ├── partials/
│       └── base.html
├── config.py
├── requirements.txt
├── README.md
└── run.py
```

## Setup

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python run.py
```

Open in browser:

```text
http://127.0.0.1:5000/
```

## Notes

- Database file is created automatically at `instance/users.db`.
- Change the `SECRET_KEY` in `config.py` or set it through environment variables for production.
- This is a strong frontend/auth foundation for adding scraping and processing modules next.
