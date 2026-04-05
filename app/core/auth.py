from datetime import datetime, timedelta
from typing import Optional
from jose import JWTError, jwt
from passlib.context import CryptContext
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.orm import Session
from .database import get_db, settings
from ..models import base as models
from sqlalchemy.ext.asyncio import AsyncSession
pwd_context = CryptContext(schemes=["pbkdf2_sha256", "bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login")

import hashlib


def _is_sha256_hash(value: str) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(ch in '0123456789abcdef' for ch in value.lower())

def verify_password(plain_password, hashed_password):
    if _is_sha256_hash(hashed_password):
        return hashlib.sha256(plain_password.encode()).hexdigest() == hashed_password
    try:
        return pwd_context.verify(plain_password, hashed_password)
    except Exception:
        return hashlib.sha256(plain_password.encode()).hexdigest() == hashed_password

def get_password_hash(password):
    try:
        return pwd_context.hash(password)
    except Exception:
        return hashlib.sha256(password.encode()).hexdigest()

import uuid

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)
    return encoded_jwt

def create_refresh_token(data: dict):
    to_encode = data.copy()
    jti = str(uuid.uuid4())
    expire = datetime.utcnow() + timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS)
    to_encode.update({"exp": expire, "jti": jti})
    encoded_jwt = jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)
    return encoded_jwt, jti, expire

async def save_refresh_token(db: AsyncSession, user_id: int, jti: str, expires_at: datetime):
    new_token = models.RefreshToken(token_id=jti, user_id=user_id, expires_at=expires_at)
    db.add(new_token)
    await db.commit()

async def revoke_refresh_token(db: AsyncSession, jti: str):
    result = await db.execute(select(models.RefreshToken).filter(models.RefreshToken.token_id == jti))
    token = result.scalars().first()
    if token:
        token.revoked_at = datetime.utcnow()
        await db.commit()

async def is_refresh_token_valid(db: AsyncSession, jti: str) -> bool:
    result = await db.execute(
        select(models.RefreshToken)
        .filter(models.RefreshToken.token_id == jti)
        .filter(models.RefreshToken.revoked_at == None)
        .filter(models.RefreshToken.expires_at > datetime.utcnow())
    )
    return result.scalars().first() is not None

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

async def get_current_user(token: str = Depends(oauth2_scheme), db: AsyncSession = Depends(get_db)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    
    from sqlalchemy.orm import joinedload
    result = await db.execute(select(models.User).options(joinedload(models.User.role)).filter(models.User.username == username))
    user = result.scalars().first()
    
    if user is None:
        raise credentials_exception
    return user
