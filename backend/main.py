from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from database import engine, get_db
from models import Base
import crud, schemas, auth

# ─── INIT APP ─────────────────────────────────────────────────────────────────

Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="EduSmart ERP",
    description="Système ERP pour centre de formation",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── AUTH ─────────────────────────────────────────────────────────────────────

@app.post("/api/v1/auth/login", response_model=schemas.Token, tags=["Auth"])
def login(form: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    user = crud.get_user_by_email(db, form.username)
    if not user or not auth.verify_password(form.password, user.hashed_password):
        raise HTTPException(status_code=401, detail="Email ou mot de passe incorrect")
    token = auth.create_access_token({"sub": user.email, "role": user.role})
    return {"access_token": token, "token_type": "bearer"}

@app.get("/api/v1/auth/me", response_model=schemas.UserOut, tags=["Auth"])
def me(current_user=Depends(auth.get_current_user)):
    return current_user


# ─── USERS ────────────────────────────────────────────────────────────────────

@app.post("/api/v1/users", response_model=schemas.UserOut, tags=["Users"])
def create_user(data: schemas.UserCreate, db: Session = Depends(get_db),
                _=Depends(auth.require_role("admin"))):
    if crud.get_user_by_email(db, data.email):
        raise HTTPException(status_code=400, detail="Email déjà utilisé")
    return crud.create_user(db, data)

@app.get("/api/v1/users", response_model=list[schemas.UserOut], tags=["Users"])
def list_users(db: Session = Depends(get_db), _=Depends(auth.require_role("admin", "scolarite"))):
    return crud.get_users(db)


# ─── SEMESTERS ────────────────────────────────────────────────────────────────

@app.post("/api/v1/semesters", response_model=schemas.SemesterOut, tags=["Semesters"])
def create_semester(data: schemas.SemesterCreate, db: Session = Depends(get_db),
                    _=Depends(auth.require_role("admin", "scolarite"))):
    return crud.create_semester(db, data)

@app.get("/api/v1/semesters", response_model=list[schemas.SemesterOut], tags=["Semesters"])
def list_semesters(db: Session = Depends(get_db), _=Depends(auth.get_current_user)):
    return crud.get_semesters(db)


# ─── STUDENTS ─────────────────────────────────────────────────────────────────

@app.post("/api/v1/students", response_model=schemas.StudentOut, tags=["Students"])
def create_student(data: schemas.StudentCreate, db: Session = Depends(get_db),
                   _=Depends(auth.require_role("admin", "scolarite"))):
    return crud.create_student(db, data)

@app.get("/api/v1/students", response_model=list[schemas.StudentOut], tags=["Students"])
def list_students(db: Session = Depends(get_db), _=Depends(auth.require_role("admin", "scolarite", "formateur"))):
    return crud.get_students(db)

@app.get("/api/v1/students/{student_id}", response_model=schemas.StudentOut, tags=["Students"])
def get_student(student_id: int, db: Session = Depends(get_db), _=Depends(auth.get_current_user)):
    s = crud.get_student(db, student_id)
    if not s:
        raise HTTPException(status_code=404, detail="Étudiant introuvable")
    return s


# ─── TEACHERS ─────────────────────────────────────────────────────────────────

@app.post("/api/v1/teachers", response_model=schemas.TeacherOut, tags=["Teachers"])
def create_teacher(data: schemas.TeacherCreate, db: Session = Depends(get_db),
                   _=Depends(auth.require_role("admin"))):
    return crud.create_teacher(db, data)

@app.get("/api/v1/teachers", response_model=list[schemas.TeacherOut], tags=["Teachers"])
def list_teachers(db: Session = Depends(get_db), _=Depends(auth.get_current_user)):
    return crud.get_teachers(db)


# ─── COURSES ──────────────────────────────────────────────────────────────────

@app.post("/api/v1/courses", response_model=schemas.CourseOut, tags=["Courses"])
def create_course(data: schemas.CourseCreate, db: Session = Depends(get_db),
                  _=Depends(auth.require_role("admin", "scolarite"))):
    return crud.create_course(db, data)

@app.get("/api/v1/courses", response_model=list[schemas.CourseOut], tags=["Courses"])
def list_courses(db: Session = Depends(get_db), _=Depends(auth.get_current_user)):
    return crud.get_courses(db)

@app.get("/api/v1/courses/{course_id}", response_model=schemas.CourseOut, tags=["Courses"])
def get_course(course_id: int, db: Session = Depends(get_db), _=Depends(auth.get_current_user)):
    c = crud.get_course(db, course_id)
    if not c:
        raise HTTPException(status_code=404, detail="Cours introuvable")
    return c


# ─── ENROLLMENTS ──────────────────────────────────────────────────────────────

@app.post("/api/v1/enrollments", response_model=schemas.EnrollmentOut, tags=["Enrollments"])
def create_enrollment(data: schemas.EnrollmentCreate, db: Session = Depends(get_db),
                      current_user=Depends(auth.require_role("admin", "scolarite", "etudiant"))):
    return crud.create_enrollment(db, data, current_user.id)

@app.patch("/api/v1/enrollments/{enrollment_id}", response_model=schemas.EnrollmentOut, tags=["Enrollments"])
def update_enrollment(enrollment_id: int, data: schemas.EnrollmentUpdate,
                      db: Session = Depends(get_db),
                      current_user=Depends(auth.require_role("admin", "scolarite"))):
    return crud.update_enrollment_status(db, enrollment_id, data, current_user.id)

@app.get("/api/v1/enrollments", response_model=list[schemas.EnrollmentOut], tags=["Enrollments"])
def list_enrollments(student_id: int = None, course_id: int = None,
                     db: Session = Depends(get_db), _=Depends(auth.get_current_user)):
    return crud.get_enrollments(db, student_id, course_id)


# ─── GRADES ───────────────────────────────────────────────────────────────────

@app.post("/api/v1/grades", response_model=schemas.GradeOut, tags=["Grades"])
def create_grade(data: schemas.GradeCreate, db: Session = Depends(get_db),
                 current_user=Depends(auth.require_role("admin", "formateur"))):
    return crud.create_grade(db, data, current_user.id)

@app.patch("/api/v1/grades/{grade_id}", response_model=schemas.GradeOut, tags=["Grades"])
def update_grade(grade_id: int, data: schemas.GradeUpdate,
                 db: Session = Depends(get_db),
                 current_user=Depends(auth.require_role("admin", "formateur"))):
    return crud.update_grade(db, grade_id, data, current_user.id)

@app.get("/api/v1/grades", response_model=list[schemas.GradeOut], tags=["Grades"])
def list_grades(enrollment_id: int = None, db: Session = Depends(get_db),
                _=Depends(auth.get_current_user)):
    return crud.get_grades(db, enrollment_id)


# ─── AUDIT ────────────────────────────────────────────────────────────────────

@app.get("/api/v1/audit", tags=["Audit"])
def get_audit_logs(db: Session = Depends(get_db),
                   _=Depends(auth.require_role("admin"))):
    return crud.get_audit_logs(db)


# ─── HEALTH ───────────────────────────────────────────────────────────────────

@app.get("/", tags=["Health"])
def root():
    return {"status": "EduSmart API running", "docs": "/docs"}