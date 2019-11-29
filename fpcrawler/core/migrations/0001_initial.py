# Generated by Django 2.1.7 on 2019-03-26 07:31

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Task',
            fields=[
                ('nk', models.CharField(db_index=True, max_length=100, primary_key=True, serialize=False, unique=True)),
                ('progress', models.IntegerField()),
                ('name', models.CharField(max_length=200)),
                ('identifier', models.CharField(max_length=200)),
                ('complete', models.BooleanField(default=False)),
                ('regDt', models.DateTimeField(auto_now_add=True)),
                ('modDt', models.DateTimeField(auto_now=True)),
            ],
            options={
                'db_table': 'task',
            },
        ),
    ]
