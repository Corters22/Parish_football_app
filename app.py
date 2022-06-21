import os
from flask import Flask, render_template, request, url_for, redirect, flash
from flask_sqlalchemy import SQLAlchemy
import pandas as pd
#from flask_login import login_required, LoginManager

from sqlalchemy.sql import func

from werkzeug.utils import secure_filename

from sqlalchemy import create_engine, func, Column, Integer, String, Date

from flask import Flask
from flask_marshmallow import Marshmallow
from marshmallow import Schema, fields
#from pprint import pprint

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from config import user, password, port, db, username, app_password


import functions

app = Flask(__name__)

#connect to database


url = f'postgresql://{user}:{password}@localhost:{port}/{db}'
engine = create_engine(url)
app.config['SQLALCHEMY_DATABASE_URI'] = url

db = SQLAlchemy(app)


# UPLOAD_FOLDER = 'static/uploads'
# app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

Base = declarative_base()

Session = sessionmaker(bind=engine)
session = Session

#create table model
class Football(db.Model):
    __tablename__ = 'football_data'
    
    id = Column(Integer, primary_key = True)
    qtr = Column(Integer)
    play_no = Column(Integer)
    dn = Column(Integer)
    dist = Column(Integer)
    yard_ln = Column(Integer)
    off_form = Column(String)
    def_front = Column(String)
    stunt = Column(String)
    blitz = Column(String)
    coverage = Column(String)
    field_position = Column(String)
    distance = Column(String)
    date_of_game = Column(Date)
    opponent = Column(String)
    
    
    def __repr__(self):
        return "<Football(qtr='%s', play_no='%s', dn='%s', dist='%s', yard_ln='%s', off_form='%s', def_front='%s', stunt='%s', blitz='%s', field_position='%s', distance='%s', date_of_game='%s', opponent='%s')>" % (self.qtr, self.play_no, self.dn, self.dist, self.yard_ln, self.off_form, self.def_front, self.stunt, self.blitz, self.field_position, self.distance, self.date_of_game, self.opponent)

#Base.metadata.create_all(engine)
# db.create_all()
# db.session.add(Football())
# db.session.commit()

#create marshmallow model for jsonify results
class FootballSchema(Schema):
    id = fields.Int()
    qtr = fields.Int()
    play_no = fields.Int()
    dn = fields.Int()
    dist = fields.Int()
    yard_ln = fields.Int()
    off_form = fields.Str()
    def_front = fields.Str()
    stunt = fields.Str()
    blitz = fields.Str()
    coverage = fields.Str()
    field_position = fields.Str()
    distance = fields.Str()
    date_of_game = fields.Date()
    opponent = fields.Str()

schema = FootballSchema(many=True)

#manage login information
# login_manager = LoginManager()
# login_manager.init_app(app)

@app.route('/login', methods = ['GET', 'POST'])
def login():
    error=None
    if request.method == 'POST':
        if request.form['username'] != username or request.form['password'] != app_password:
            error='Invalid Credentials. Please try again.'
        else:
            query = db.session.query(Football)
            data = pd.read_sql(query.statement, db.session.connection())
            return redirect(url_for('full_data', column_names = data.columns.values, 
                row_data = list(data.values.tolist()), zip = zip))
            
    return render_template('login.html', error=error)

@app.route('/home')
#@login_required
def full_data():
    query = db.session.query(Football)
    data = pd.read_sql(query.statement, db.session.connection())
    return render_template('home.html', column_names = data.columns.values, 
            row_data = list(data.values.tolist()), zip = zip)


@app.route('/home', methods = ['GET', 'POST'])
#@login_required
def filter_data():
    if request.method == 'POST':
        school = request.form['opponent']
        start_date = request.form['date1']
        end_date = request.form['date2']
        defensive_front = request.form['defensivefront']
        blitz = request.form['blitz']
        down = request.form['down']
        distance = request.form['distance']
        field_position = request.form['fieldposition']

        print('school=', school, 'start_date=', start_date, 'end_date=', end_date, 
            'defensive_front=', defensive_front, 'blitz=', blitz, 'down=', down,
            'distance=', distance, 'field_position=', field_position)

        data = pd.DataFrame(functions.filter_function(school, start_date, end_date, defensive_front, blitz, down, distance, field_position))

        blitz_data = pd.DataFrame(functions.blitz_stats(data))
        stunt_data = pd.DataFrame(functions.stunt_stats(data))
        coverage_data = pd.DataFrame(functions.coverage_stats(data))


    return render_template('home.html', column_names = data.columns.values, 
                row_data = list(data.values.tolist()), zip = zip, blitz_column_names = blitz_data.columns.values, 
                blitz_row_data = list(blitz_data.values.tolist()), stunt_column_names = stunt_data.columns.values, 
                stunt_row_data = list(stunt_data.values.tolist()), coverage_column_names = coverage_data.columns.values, 
                coverage_row_data = list(coverage_data.values.tolist()))

# @app.route('/upload-data', methods = ['GET', 'POST'])
# def add_data():
#     if request.method == 'POST':
#         new_file = request.files['new_file']
#         new_date = request.form['new_date']
#         new_opponent = request.form['new_opponent']
#         #filename = secure_filename(new_file.filename)
#         if new_file:
#             with open(new_file) as file:
#             #new_file.save(os.path.join(app.config('UPLOAD_FOLDER'), 'New_file'))
            #file = pd.read_excel(files_excel(new_file))
#         # print(new_date, new_opponent)

#                 file2 = functions.add_binned_columns(file)

#                 functions.add_data(file2, new_date, new_opponent)

#                 flash('File successfully uploaded ' + file.filename + ' to the database!')
#             return redirect('/home')
#         else: 
#             flash('Invalid Upload xlx')
        
#         return render_template('uploadexcel.html')


if __name__ == '__main__':
    app.run(debug=True)