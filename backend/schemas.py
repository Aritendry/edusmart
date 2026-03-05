from pydantic import BaseModel, EmailStr
from typing import Optional
from datetime import datetime
from models import RoleEnum, EnrollmentStatus


# ─── USER ─────────────────────────────────────────────────────────────────────

class UserCreate(BaseModel):
    email: EmailStr
    password: str
    role: RoleEnum

class UserOut(BaseModel):
    id: int
    email: EmailStr
    role: RoleEnum
    is_active: bool

    class Config:
        from_attributes = True

class Token(BaseModel):
    access_token: str
    token_type: str


# ─── SEMESTER ─────────────────────────────────────────────────────────────────

class SemesterCreate(BaseModel):
    name: str
    start_date: datetime
    end_date: datetime
    enroll_deadline: datetime
    exam_start: Optional[datetime] = None
    exam_end: Optional[datetime] = None

class SemesterOut(SemesterCreate):
    id: int
    class Config:
        from_attributes = True


# ─── STUDENT ──────────────────────────────────────────────────────────────────

class StudentCreate(BaseModel):
    email: EmailStr
    password: str
    first_name: str
    last_name: str
    birth_date: Optional[datetime] = None
    phone: Optional[str] = None
    address: Optional[str] = None

class StudentOut(BaseModel):
    id: int
    first_name: str
    last_name: str
    phone: Optional[str]
    user: UserOut

    class Config:
        from_attributes = True


# ─── TEACHER ──────────────────────────────────────────────────────────────────

class TeacherCreate(BaseModel):
    email: EmailStr
    password: str
    first_name: str
    last_name: str
    specialty: Optional[str] = None
    phone: Optional[str] = None

class TeacherOut(BaseModel):
    id: int
    first_name: str
    last_name: str
    specialty: Optional[str]
    user: UserOut

    class Config:
        from_attributes = True


# ─── COURSE ───────────────────────────────────────────────────────────────────

class CourseCreate(BaseModel):
    code: str
    name: str
    description: Optional[str] = None
    credits: int = 3
    quota: int = 30
    teacher_id: int
    semester_id: int

class CourseOut(BaseModel):
    id: int
    code: str
    name: str
    credits: int
    quota: int
    teacher_id: int
    semester_id: int

    class Config:
        from_attributes = True


# ─── ENROLLMENT ───────────────────────────────────────────────────────────────

class EnrollmentCreate(BaseModel):
    student_id: int
    course_id: int

class EnrollmentUpdate(BaseModel):
    status: EnrollmentStatus

class EnrollmentOut(BaseModel):
    id: int
    student_id: int
    course_id: int
    status: EnrollmentStatus
    enrolled_at: datetime

    class Config:
        from_attributes = True


# ─── GRADE ────────────────────────────────────────────────────────────────────

class GradeCreate(BaseModel):
    enrollment_id: int
    score: float
    comment: Optional[str] = None

class GradeUpdate(BaseModel):
    score: float
    comment: Optional[str] = None

class GradeOut(BaseModel):
    id: int
    enrollment_id: int
    score: float
    comment: Optional[str]
    graded_at: datetime

    class Config:
        from_attributes = True