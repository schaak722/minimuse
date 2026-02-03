# Mini Muse Costings (Phase 0)

## What you have
- Flask app scaffold
- Login/logout
- Roles: admin/user/viewer
- Admin user management
- Shopify-like admin layout

## Koyeb env vars (required)
- DATABASE_URL (Neon connection string using postgresql+psycopg)
- SECRET_KEY
- ADMIN_EMAIL
- ADMIN_PASSWORD
- ADMIN_NAME (optional)

## Start command (Koyeb)
gunicorn wsgi:app --bind 0.0.0.0:$PORT

