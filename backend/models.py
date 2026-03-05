from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, ForeignKey, Enum, Text
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from database import Base
import enum


# ─── ENUMS ────────────────────────────────────────────────────────────────────

class RoleEnum(str, enum.Enum):
    admin      = "admin"
    formateur  = "formateur"
    etudiant   = "etudiant"
    scolarite  = "scolarite"

class EnrollmentStatus(str, enum.Enum):
    en_attente = "en_attente"
    validee    = "validee"
    annulee    = "annulee"


# ─── USER ─────────────────────────────────────────────────────────────────────

class User(Base):
    __tablename__ = "users"

    id            = Column(Integer, primary_key=True, index=True)
    email         = Column(String, unique=True, index=True, nullable=False)
    hashed_password = Column(String, nullable=False)
    role          = Column(Enum(RoleEnum), nullable=False)
    is_active     = Column(Boolean, default=True)
    created_at    = Column(DateTime(timezone=True), server_default=func.now())

    student       = relationship("Student", back_populates="user", uselist=False)
    teacher       = relationship("Teacher", back_populates="user", uselist=False)


# ─── SEMESTRE ─────────────────────────────────────────────────────────────────

class Semester(Base):
    __tablename__ = "semesters"

    id         = Column(Integer, primary_key=True, index=True)
    name       = Column(String, nullable=False)          # ex: "S1 2024-2025"
    start_date = Column(DateTime(timezone=True), nullable=False)
    end_date   = Column(DateTime(timezone=True), nullable=False)
    enroll_deadline = Column(DateTime(timezone=True), nullable=False)
    exam_start = Column(DateTime(timezone=True))
    exam_end   = Column(DateTime(timezone=True))

    courses    = relationship("Course", back_populates="semester")


# ─── ÉTUDIANT ─────────────────────────────────────────────────────────────────

class Student(Base):
    __tablename__ = "students"

    id         = Column(Integer, primary_key=True, index=True)
    user_id    = Column(Integer, ForeignKey("users.id"), unique=True)
    first_name = Column(String, nullable=False)
    last_name  = Column(String, nullable=False)
    birth_date = Column(DateTime)
    phone      = Column(String)
    address    = Column(String)

    user        = relationship("User", back_populates="student")
    enrollments = relationship("Enrollment", back_populates="student")


# ─── FORMATEUR ────────────────────────────────────────────────────────────────

class Teacher(Base):
    __tablename__ = "teachers"

    id         = Column(Integer, primary_key=True, index=True)
    user_id    = Column(Integer, ForeignKey("users.id"), unique=True)
    first_name = Column(String, nullable=False)
    last_name  = Column(String, nullable=False)
    specialty  = Column(String)
    phone      = Column(String)

    user       = relationship("User", back_populates="teacher")
    courses    = relationship("Course", back_populates="teacher")


# ─── COURS / MODULE ───────────────────────────────────────────────────────────

class Course(Base):
    __tablename__ = "courses"

    id          = Column(Integer, primary_key=True, index=True)
    code        = Column(String, unique=True, nullable=False)   # ex: "INF301"
    name        = Column(String, nullable=False)
    description = Column(Text)
    credits     = Column(Integer, default=3)
    quota       = Column(Integer, default=30)                   # places max
    teacher_id  = Column(Integer, ForeignKey("teachers.id"))
    semester_id = Column(Integer, ForeignKey("semesters.id"))

    teacher     = relationship("Teacher", back_populates="courses")
    semester    = relationship("Semester", back_populates="courses")
    enrollments = relationship("Enrollment", back_populates="course")


# ─── INSCRIPTION ──────────────────────────────────────────────────────────────

class Enrollment(Base):
    __tablename__ = "enrollments"

    id          = Column(Integer, primary_key=True, index=True)
    student_id  = Column(Integer, ForeignKey("students.id"), nullable=False)
    course_id   = Column(Integer, ForeignKey("courses.id"), nullable=False)
    status      = Column(Enum(EnrollmentStatus), default=EnrollmentStatus.en_attente)
    enrolled_at = Column(DateTime(timezone=True), server_default=func.now())

    student     = relationship("Student", back_populates="enrollments")
    course      = relationship("Course", back_populates="enrollments")
    grade       = relationship("Grade", back_populates="enrollment", uselist=False)


# ─── NOTE ─────────────────────────────────────────────────────────────────────

class Grade(Base):
    __tablename__ = "grades"

    id            = Column(Integer, primary_key=True, index=True)
    enrollment_id = Column(Integer, ForeignKey("enrollments.id"), unique=True, nullable=False)
    score         = Column(Float, nullable=False)        # /20
    comment       = Column(Text)
    graded_at     = Column(DateTime(timezone=True), server_default=func.now())

    enrollment    = relationship("Enrollment", back_populates="grade")


# ─── AUDIT LOG (traçabilité) ──────────────────────────────────────────────────

class AuditLog(Base):
    __tablename__ = "audit_logs"

    id          = Column(Integer, primary_key=True, index=True)
    user_id     = Column(Integer, ForeignKey("users.id"))
    action      = Column(String, nullable=False)         # ex: "UPDATE_GRADE"
    entity      = Column(String)                         # ex: "Grade"
    entity_id   = Column(Integer)
    detail      = Column(Text)                           # JSON ou description
    created_at  = Column(DateTime(timezone=True), server_default=func.now())