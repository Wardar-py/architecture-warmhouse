import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Optional

import asyncpg
import jwt
from fastapi import Depends, FastAPI, HTTPException
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from passlib.context import CryptContext
from starlette import status

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "auth-db"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "auth_service"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "password"),
}
SECRET_KEY = os.getenv("SECRET_KEY", "your-secret-key-here")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "30"))

DEPS = {}
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


def get_password_hash(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)


def get_db():
    return DEPS["db"]


async def get_user_by_email(email: str):
    pool = DEPS["db"]
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM users WHERE email = $1", email)
        return row


async def authenticate_user(email: str, password: str):
    user = await get_user_by_email(email)
    if not user:
        return False
    if not verify_password(password, user["hashed_password"]):
        return False
    return user


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.JWT().encode(payload=to_encode, key=SECRET_KEY, alg=ALGORITHM)
    return encoded_jwt


async def get_user_by_id(user_id: str):
    pool = DEPS["db"]
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM users WHERE id = $1", user_id)
        return row


async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.JWT().decode(message=token, key=SECRET_KEY, algorithms={ALGORITHM})
        user_id: str = payload.get("sub")
        if user_id is None:
            raise credentials_exception
    except Exception:
        raise credentials_exception
    user = await get_user_by_id(user_id)
    if user is None:
        raise credentials_exception
    return user


async def init_db():
    conn = await asyncpg.connect(**DB_CONFIG)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id VARCHAR(36) PRIMARY KEY,
            email VARCHAR(255) UNIQUE NOT NULL,
            username VARCHAR(100) UNIQUE NOT NULL,
            full_name VARCHAR(255),
            hashed_password TEXT NOT NULL,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS user_sessions (
            id VARCHAR(36) PRIMARY KEY,
            user_id VARCHAR(36) REFERENCES users(id),
            token TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            expires_at TIMESTAMP NOT NULL,
            is_active BOOLEAN DEFAULT TRUE
        );
    """)
    await conn.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    db_pool = await asyncpg.create_pool(**DB_CONFIG)
    DEPS["db"] = db_pool
    yield
    await db_pool.close()


app = FastAPI(
    title="Authentication Service",
    description="Сервис аутентификации и авторизации",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health_check():
    return {"status": "healthy"}


@app.post("/auth/login")
async def login(form_data: OAuth2PasswordRequestForm = Depends(), pool=Depends(get_db)):
    user = await authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user["id"], "email": user["email"]}, expires_delta=access_token_expires
    )
    async with pool.acquire() as conn:
        session_id = str(uuid.uuid4())
        expires_at = datetime.utcnow() + access_token_expires
        await conn.execute(
            "INSERT INTO user_sessions (id, user_id, token, expires_at) VALUES ($1, $2, $3, $4)",
            session_id, user["id"], access_token, expires_at,
        )
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "expires_in": ACCESS_TOKEN_EXPIRE_MINUTES * 60,
        "user_id": user["id"],
    }


@app.post("/auth/register")
async def register_user(user_data: dict, pool=Depends(get_db)):
    email = user_data.get("email")
    username = user_data.get("username")
    password = user_data.get("password")
    full_name = user_data.get("full_name")
    existing_user = await get_user_by_email(email)
    if existing_user:
        raise HTTPException(status_code=400, detail="Email already registered")
    user_id = str(uuid.uuid4())
    hashed_password = get_password_hash(password)
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO users (id, email, username, full_name, hashed_password) VALUES ($1, $2, $3, $4, $5)",
            user_id, email, username, full_name, hashed_password,
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(data={"sub": user_id, "email": email}, expires_delta=access_token_expires)
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "expires_in": ACCESS_TOKEN_EXPIRE_MINUTES * 60,
        "user_id": user_id,
    }


@app.get("/auth/users/me")
async def read_users_me(current_user: dict = Depends(get_current_user)):
    return {
        "id": current_user["id"],
        "email": current_user["email"],
        "username": current_user["username"],
        "full_name": current_user["full_name"],
        "is_active": current_user["is_active"],
    }


@app.post("/auth/internal/validate")
async def validate_token(request: dict, pool=Depends(get_db)):
    token = request.get("token")
    if not token:
        raise HTTPException(status_code=400, detail="Token required")
    try:
        payload = jwt.JWT().decode(message=token, key=SECRET_KEY, algorithms={ALGORITHM})
        user_id = payload.get("sub")
        async with pool.acquire() as conn:
            user = await conn.fetchrow("SELECT * FROM users WHERE id = $1 AND is_active = TRUE", user_id)
            if not user:
                raise HTTPException(status_code=401, detail="User not found or inactive")
            session = await conn.fetchrow(
                "SELECT * FROM user_sessions WHERE token = $1 AND is_active = TRUE AND expires_at > NOW()", token
            )
            if not session:
                raise HTTPException(status_code=401, detail="Invalid or expired session")
        return {"valid": True, "user_id": user_id, "email": payload.get("email"), "expires_at": payload.get("exp")}
    except Exception:
        return {"valid": False, "error": "Invalid token"}


@app.post("/auth/logout")
async def logout(pool=Depends(get_db), _: dict = Depends(get_current_user), token: str = Depends(oauth2_scheme)):
    async with pool.acquire() as conn:
        await conn.execute("UPDATE user_sessions SET is_active = FALSE WHERE token = $1", token)
    return {"message": "Successfully logged out"}
