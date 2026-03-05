from sqlalchemy.orm import Session
from sqlalchemy import func
from models import User, Student, Teacher, Course, Enrollment, Grade, Semester, AuditLog, EnrollmentStatus
from schemas import (UserCreate, StudentCreate, TeacherCreate, CourseCreate,
                     EnrollmentCreate, EnrollmentUpdate, GradeCreate, GradeUpdate, SemesterCreate)
from auth import get_password_hash
from datetime import datetime, timezone
from fastapi import HTTPException


# ─── AUDIT ────────────────────────────────────────────────────────────────────

def log_action(db: Session, user_id: int, action: str, entity: str, entity_id: int, detail: str = ""):
    log = AuditLog(user_id=user_id, action=action, entity=entity, entity_id=entity_id, detail=detail)
    db.add(log)
    db.commit()


# ─── USER ─────────────────────────────────────────────────────────────────────

def get_user_by_email(db: Session, email: str):
    return db.query(User).filter(User.email == email).first()

def create_user(db: Session, data: UserCreate):
    user = User(email=data.email, hashed_password=get_password_hash(data.password), role=data.role)
    db.add(user)
    db.commit()
    db.refresh(user)
    return user

def get_users(db: Session):
    return db.query(User).all()


# ─── SEMESTER ─────────────────────────────────────────────────────────────────

def create_semester(db: Session, data: SemesterCreate):
    semester = Semester(**data.model_dump())
    db.add(semester)
    db.commit()
    db.refresh(semester)
    return semester

def get_semesters(db: Session):
    return db.query(Semester).all()

def get_semester(db: Session, semester_id: int):
    return db.query(Semester).filter(Semester.id == semester_id).first()


# ─── STUDENT ──────────────────────────────────────────────────────────────────

def create_student(db: Session, data: StudentCreate):
    user = User(email=data.email, hashed_password=get_password_hash(data.password), role="etudiant")
    db.add(user)
    db.commit()
    db.refresh(user)
    student = Student(
        user_id=user.id,
        first_name=data.first_name,
        last_name=data.last_name,
        birth_date=data.birth_date,
        phone=data.phone,
        address=data.address
    )
    db.add(student)
    db.commit()
    db.refresh(student)
    return student

def get_students(db: Session):
    return db.query(Student).all()

def get_student(db: Session, student_id: int):
    return db.query(Student).filter(Student.id == student_id).first()


# ─── TEACHER ──────────────────────────────────────────────────────────────────

def create_teacher(db: Session, data: TeacherCreate):
    user = User(email=data.email, hashed_password=get_password_hash(data.password), role="formateur")
    db.add(user)
    db.commit()
    db.refresh(user)
    teacher = Teacher(
        user_id=user.id,
        first_name=data.first_name,
        last_name=data.last_name,
        specialty=data.specialty,
        phone=data.phone
    )
    db.add(teacher)
    db.commit()
    db.refresh(teacher)
    return teacher

def get_teachers(db: Session):
    return db.query(Teacher).all()

def get_teacher(db: Session, teacher_id: int):
    return db.query(Teacher).filter(Teacher.id == teacher_id).first()


# ─── COURSE ───────────────────────────────────────────────────────────────────

def create_course(db: Session, data: CourseCreate):
    course = Course(**data.model_dump())
    db.add(course)
    db.commit()
    db.refresh(course)
    return course

def get_courses(db: Session):
    return db.query(Course).all()

def get_course(db: Session, course_id: int):
    return db.query(Course).filter(Course.id == course_id).first()


# ─── ENROLLMENT ───────────────────────────────────────────────────────────────

def create_enrollment(db: Session, data: EnrollmentCreate, current_user_id: int):
    # Règle : pas de double inscription
    existing = db.query(Enrollment).filter(
        Enrollment.student_id == data.student_id,
        Enrollment.course_id == data.course_id
    ).first()
    if existing:
        raise HTTPException(status_code=400, detail="Étudiant déjà inscrit à ce cours")

    # Règle : quota
    course = db.query(Course).filter(Course.id == data.course_id).first()
    if not course:
        raise HTTPException(status_code=404, detail="Cours introuvable")

    count = db.query(func.count(Enrollment.id)).filter(
        Enrollment.course_id == data.course_id,
        Enrollment.status != EnrollmentStatus.annulee
    ).scalar()

    if count >= course.quota:
        raise HTTPException(status_code=400, detail=f"Quota atteint ({course.quota} places)")

    # Règle : deadline inscription
    semester = db.query(Semester).filter(Semester.id == course.semester_id).first()
    if semester and datetime.now(timezone.utc) > semester.enroll_deadline.replace(tzinfo=timezone.utc):
        raise HTTPException(status_code=400, detail="Date limite d'inscription dépassée")

    enrollment = Enrollment(student_id=data.student_id, course_id=data.course_id)
    db.add(enrollment)
    db.commit()
    db.refresh(enrollment)
    log_action(db, current_user_id, "CREATE_ENROLLMENT", "Enrollment", enrollment.id)
    return enrollment

def update_enrollment_status(db: Session, enrollment_id: int, data: EnrollmentUpdate, current_user_id: int):
    enrollment = db.query(Enrollment).filter(Enrollment.id == enrollment_id).first()
    if not enrollment:
        raise HTTPException(status_code=404, detail="Inscription introuvable")
    enrollment.status = data.status
    db.commit()
    db.refresh(enrollment)
    log_action(db, current_user_id, f"UPDATE_ENROLLMENT_STATUS:{data.status}", "Enrollment", enrollment_id)
    return enrollment

def get_enrollments(db: Session, student_id: int = None, course_id: int = None):
    q = db.query(Enrollment)
    if student_id:
        q = q.filter(Enrollment.student_id == student_id)
    if course_id:
        q = q.filter(Enrollment.course_id == course_id)
    return q.all()


# ─── GRADE ────────────────────────────────────────────────────────────────────

def create_grade(db: Session, data: GradeCreate, current_user_id: int):
    # Règle : l'inscription doit exister et être validée
    enrollment = db.query(Enrollment).filter(Enrollment.id == data.enrollment_id).first()
    if not enrollment:
        raise HTTPException(status_code=404, detail="Inscription introuvable")
    if enrollment.status != EnrollmentStatus.validee:
        raise HTTPException(status_code=400, detail="Impossible de noter une inscription non validée")

    # Règle : pas de double note
    if db.query(Grade).filter(Grade.enrollment_id == data.enrollment_id).first():
        raise HTTPException(status_code=400, detail="Note déjà attribuée pour cette inscription")

    if not (0 <= data.score <= 20):
        raise HTTPException(status_code=400, detail="La note doit être entre 0 et 20")

    grade = Grade(**data.model_dump())
    db.add(grade)
    db.commit()
    db.refresh(grade)
    log_action(db, current_user_id, "CREATE_GRADE", "Grade", grade.id, f"score={data.score}")
    return grade

def update_grade(db: Session, grade_id: int, data: GradeUpdate, current_user_id: int):
    grade = db.query(Grade).filter(Grade.id == grade_id).first()
    if not grade:
        raise HTTPException(status_code=404, detail="Note introuvable")
    if not (0 <= data.score <= 20):
        raise HTTPException(status_code=400, detail="La note doit être entre 0 et 20")

    old_score = grade.score
    grade.score = data.score
    grade.comment = data.comment
    db.commit()
    db.refresh(grade)
    log_action(db, current_user_id, "UPDATE_GRADE", "Grade", grade_id, f"{old_score} → {data.score}")
    return grade

def get_grades(db: Session, enrollment_id: int = None):
    q = db.query(Grade)
    if enrollment_id:
        q = q.filter(Grade.enrollment_id == enrollment_id)
    return q.all()


# ─── AUDIT LOG ────────────────────────────────────────────────────────────────

def get_audit_logs(db: Session):
    return db.query(AuditLog).order_by(AuditLog.created_at.desc()).limit(100).all()