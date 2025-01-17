from django.db import models
from django.contrib.auth.models import AbstractBaseUser, BaseUserManager

class Role(models.Model):
    staff = 'staff'
    admin = 'admin'

    ROLE_CHOICES = [
        (staff, 'staff'),
        (admin, 'admin'),
    ]

    name = models.CharField(max_length=25, choices=ROLE_CHOICES, unique=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'user_role'
        ordering = ['id']

    def __str__(self):
        return self.name


class UserManager(BaseUserManager):
    def create_user(self, email, password=None, **extra_fields):
        if not email:
            raise ValueError('The Email field must be set')
        email = self.normalize_email(email)

        # Assign 'staff' role by default (role_id=1)
        if 'role' not in extra_fields or not extra_fields['role']:
            try:
                staff_role = Role.objects.get(id=1)  # Default to role_id=1
            except Role.DoesNotExist:
                raise ValueError("Default 'staff' role (id=1) does not exist in the database.")
            extra_fields['role'] = staff_role

        user = self.model(email=email, **extra_fields)
        if password:
            user.set_password(password)
        user.save(using=self._db)
        return user



    def create_superuser(self, email, password=None, **extra_fields):
        from authentications.models import Role
        extra_fields.setdefault('is_superuser', True)
        extra_fields.setdefault('is_staff', True)
        # Optionally assign 'admin' role to superusers
        admin_role = Role.objects.filter(name='admin').first()
        if admin_role and 'role' not in extra_fields:
            extra_fields['role'] = admin_role
        return self.create_user(email, password, **extra_fields)


class User(AbstractBaseUser):
    email = models.EmailField(unique=True)
    firstname = models.CharField(max_length=255)
    lastname = models.CharField(max_length=255)
    position = models.CharField(max_length=255)
    personal_email = models.EmailField(unique=False, null=True, blank=True)
    role = models.ForeignKey(Role, on_delete=models.SET_NULL, null=True)
    department = models.CharField(max_length=255)
    section = models.CharField(max_length=255, null=True, blank=True)
    station = models.CharField(max_length=255, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    objects = UserManager()

    USERNAME_FIELD = 'email'
    REQUIRED_FIELDS = ['firstname', 'lastname']

    class Meta:
        db_table = 'user'

    def __str__(self):
        return f"{self.email} ({self.role})"
