# authentications/models.py

from django.db import models
from django.contrib.auth.models import AbstractBaseUser, BaseUserManager
from decouple import config

class Role(models.Model):
    staff = 'staff'
    signer = 'signer'
    admin = 'admin'
    signer_admin = 'signer_admin'

    ROLE_CHOICES = [
        (staff, 'staff'),
        (admin, 'admin'),
    ]

    name = models.CharField(max_length=25, choices=ROLE_CHOICES,unique=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'user_role'
        ordering = ['id']

def get_staff_role():
    Role.objects.get_or_create(name='staff')
    return Role.objects.filter(name='staff').first()




# class UserManager(BaseUserManager):
#     def create_user(self, email, password=None, **extra_fields):
#         if not email:
#             raise ValueError('The Email field must be set')
#         email = self.normalize_email(email)
#         role, created = Role.objects.get_or_create(name='staff')
#         user = self.model(email=email, role=role, **extra_fields)
#         if password:
#             user.set_password(password)
#         user.save(using=self._db)
#         return user

class UserManager(BaseUserManager):
    def create_user(self, email, password=None, **extra_fields):
        if not email:
            raise ValueError('The Email field must be set')
        email = self.normalize_email(email)

        # By default, the role will be set to 'staff'
        role, created = Role.objects.get_or_create(name='staff')
        extra_fields.setdefault('role', role)  # Set staff as the default role

        user = self.model(email=email, role=role, **extra_fields)
        if password:
            user.set_password(password)
        user.save(using=self._db)
        return user


class User(AbstractBaseUser):
    email = models.EmailField(unique=True)
    firstname = models.CharField(max_length=255)
    lastname = models.CharField(max_length=255)
    position = models.CharField(max_length=255)
    personal_email = models.EmailField(unique=True, null=True, blank=True)
    personal_email = models.EmailField(unique=False, null=True, blank=True)
    role = models.ForeignKey(Role, on_delete=models.SET_NULL, null=True,default=get_staff_role)
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
