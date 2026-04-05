from datetime import timedelta
from fastapi import APIRouter, Depends, HTTPException, status, Body, Request
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import joinedload

from ...core import auth
from ...core.database import get_db, settings
from ...core.limiter import limiter
from ...models import base as models
from ...schemas import base as schemas
from ...services.user_service import UserService

# Common responses for auth router
AUTH_RESPONSES = {
    401: {"description": "Authentication failure - Invalid credentials or expired token", "model": schemas.MessageResponse},
    429: {"description": "Too Many Requests - Rate limit exceeded (10 attempts / minute per IP)"},
}

router = APIRouter(tags=["Authentication"], responses=AUTH_RESPONSES)


@router.post(
    "/login",
    response_model=schemas.Token,
    summary="User Login",
    description="Authenticate user with credentials and return JWT tokens. Rate limited to 10 requests per minute per IP."
)
@limiter.limit("10/minute")
async def login_for_access_token(
    request: Request,
    form_data: OAuth2PasswordRequestForm = Depends(),
    db: AsyncSession = Depends(get_db)
):
    """
    Authenticate user and return access/refresh tokens.
    """
    # Fetch user with role
    result = await db.execute(
        select(models.User)
        .options(joinedload(models.User.role))
        .filter(models.User.username == form_data.username)
    )
    user = result.scalars().first()

    if not user or not auth.verify_password(form_data.password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = auth.create_access_token(
        data={"sub": user.username, "role": user.role.name if user.role else "student"},
        expires_delta=access_token_expires
    )
    refresh_token_jwt, jti, expire = auth.create_refresh_token(data={"sub": user.username})
    await auth.save_refresh_token(db, user.id, jti, expire)

    return {
        "access_token": access_token,
        "token_type": "bearer",
        "refresh_token": refresh_token_jwt,
        "expires_in": settings.ACCESS_TOKEN_EXPIRE_MINUTES * 60
    }

@router.post(
    "/refresh", 
    response_model=schemas.Token,
    summary="Refresh Access Token",
    description="Get a new access token using a valid refresh token."
)
async def refresh_access_token(refresh_token: str = Body(..., embed=True), db: AsyncSession = Depends(get_db)):
    try:
        from jose import jwt, JWTError
        payload = jwt.decode(refresh_token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
        username: str = payload.get("sub")
        jti: str = payload.get("jti")
        if username is None or jti is None:
            raise HTTPException(status_code=401, detail="Invalid refresh token")
            
        if not await auth.is_refresh_token_valid(db, jti):
             raise HTTPException(status_code=401, detail="Refresh token expired or revoked")
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid refresh token")
        
    result = await db.execute(select(models.User).options(joinedload(models.User.role)).filter(models.User.username == username))
    user = result.scalars().first()
    
    if user is None:
        raise HTTPException(status_code=401, detail="User not found")
        
    # Rotate token: revoke old one and issue new ones
    await auth.revoke_refresh_token(db, jti)
    
    access_token = auth.create_access_token(
        data={"sub": user.username, "role": user.role.name if user.role else "student"}
    )
    new_refresh_token_jwt, new_jti, new_expire = auth.create_refresh_token(data={"sub": user.username})
    await auth.save_refresh_token(db, user.id, new_jti, new_expire)

    return {
        "access_token": access_token,
        "token_type": "bearer",
        "refresh_token": new_refresh_token_jwt,
        "expires_in": settings.ACCESS_TOKEN_EXPIRE_MINUTES * 60
    }

@router.post("/logout", response_model=schemas.MessageResponse)
async def logout(refresh_token: str = Body(..., embed=True), db: AsyncSession = Depends(get_db)):
    try:
        from jose import jwt, JWTError
        payload = jwt.decode(refresh_token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
        jti: str = payload.get("jti")
        if jti:
            await auth.revoke_refresh_token(db, jti)
    except JWTError:
        pass # If token is invalid already, we consider logout successful
        
    return schemas.MessageResponse(message="Successfully logged out")

@router.get("/me", response_model=schemas.CurrentUser)
async def read_users_me(
    current_user: models.User = Depends(auth.get_current_user), 
    db: AsyncSession = Depends(get_db)
):
    """
    Get information about the currently authenticated user.
    """
    return await UserService.build_current_user_response(current_user, db)

@router.patch("/me", response_model=schemas.CurrentUser)
async def update_users_me(
    payload: schemas.ProfileUpdate,
    current_user: models.User = Depends(auth.get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Update information about the currently authenticated user.
    """
    return await UserService.update_user_profile(current_user, payload, db)

@router.post("/me/password", response_model=schemas.MessageResponse)
async def change_password(
    payload: schemas.PasswordChangeRequest, 
    current_user: models.User = Depends(auth.get_current_user), 
    db: AsyncSession = Depends(get_db)
):
    """
    Update the current user's password.
    """
    if not auth.verify_password(payload.current_password, current_user.password_hash):
        raise HTTPException(status_code=400, detail="Current password is incorrect")
    if len(payload.new_password) < 6:
        raise HTTPException(status_code=422, detail="New password must be at least 6 characters long")
    if payload.current_password == payload.new_password:
        raise HTTPException(status_code=422, detail="New password must be different from the current password")

    current_user.password_hash = auth.get_password_hash(payload.new_password)
    current_user.is_initial_password = False
    await db.commit()
    return schemas.MessageResponse(message="Password updated successfully")
