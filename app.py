import os
from flask import Flask, render_template, request, url_for, redirect, flash
from flask_sqlalchemy import SQLAlchemy
import pandas as pd
import psycopg2
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
# from config import user, password, port, db, username, app_password,uri


#import functions

app = Flask(__name__)



#connect to database

# url = 'postgres://zimnwcywuosrtt:ff518e8bd538f2c8e5663077bcfd32bbba8488cad00e0495f2642819983a5443@ec2-23-23-182-238.compute-1.amazonaws.com:5432/dcbf9qrp2js5li'
# # url = f'postgresql://{user}:{password}@localhost:{port}/{db}'

# #from heroku fixing connection to database
# uri = os.getenv(url)  # or other relevant config var
# if uri and uri.startswith("postgres://"):
#     uri = uri.replace("postgres://", "postgresql://", 1)
# #engine = create_engine(uri)
# app.config['SQLALCHEMY_DATABASE_URI'] = uri
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

#DATABASE_URL=$(heroku config:get DATABASE_URL -a your-app)
DATABASE_URL = os.environ['DATABASE_URL']

conn = psycopg2.connect(DATABASE_URL, sslmode='require')

db = SQLAlchemy(app)


# UPLOAD_FOLDER = 'static/uploads'
# app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

#Base = declarative_base()

# Session = sessionmaker(bind=engine)
# session = Session

#create table model
class Football(db.Model):
    #__tablename__ = 'football_data'
    
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

username = 'JHeath'
app_password = 'Parish!'

#manage login information
# login_manager = LoginManager()
# login_manager.init_app(app)

#function to add columns for distance and field position categories
def add_binned_columns(new_file):
    field_position_labels = ["Our mid field", "-21 to -39", "-11 to -20", "-1 to -10", "1 to 10", 
          "11 to 20", "21 to 39", "their mid field"]
    field_position_bins = [-50, -40, -21, -10, 0, 10, 20, 40, 50]
    new_file["field_position"] = pd.cut(new_file['yard_ln'], bins=field_position_bins, labels=field_position_labels)
    
    #binned column for distance
    distance_bins = [0, 3, 6, 100]
    distance_labels = ['SHORT', 'MEDIUM', 'LONG']
    new_file['distance']= pd.cut(new_file['dist'], bins=distance_bins, labels=distance_labels)
    
    return new_file


#function to add new data to database
def add_data(new_file, new_date, new_opponent):
    #from sqlalchemy import create_engine
 
    
    #conn = engine.connect()
    #conn = psycopg2.connect(uri)
    
    new_file.rename(columns={'QTR': 'qtr','PLAY #':'play_no', 'DN':'dn','DIST':'dist',
                             'YARD LN': 'yard_ln', 'OFF FORM': 'off_form', 'DEF FRONT': 'def_front',
                            'STUNT':'stunt', 'BLITZ':'blitz', 'COVERAGE':'coverage'}, inplace=True)
    
    #run adding bin function to new file
    add_binned_columns(new_file)
    
    #change to post request date
    date=new_date
    
    #change to post request opponent
    opponent=new_opponent
    
    #add posted date to uploaded file
    new_file['date_of_game'] = date
    
    #add posted opponent to uploaded file
    new_file['opponent'] = opponent
    
    new_file.to_sql('football_data', con=conn , if_exists='append', index=False)
    
    #delete blank row
    query = db.session.query(Football).filter(Football.qtr.is_(None)).delete()
    
    db.session.commit()
    db.session.close()


#function to filter data with post from html
def filter_function(school, start_date, end_date, defensive_front, blitz, down,
                   distance, field_position):
    
    import json
    
    # conn = engine.connect()
    
    query = db.session.query(Football.qtr, Football.play_no, Football.dn, Football.dist, 
                         Football.yard_ln, Football.off_form, Football.def_front, 
                         Football.stunt, Football.blitz, Football.coverage, Football.field_position, 
                         Football.distance, Football.date_of_game, Football.opponent)
    
    if school:
        query = query.filter(func.lower(Football.opponent) == func.lower(school))
    
    if start_date:
        query = query.filter(Football.date_of_game >= start_date)
    
    if end_date:
        query = query.filter(Football.date_of_game <= end_date)
    
    if defensive_front:
        query = query.filter(func.lower(Football.def_front) == func.lower(defensive_front))
        
    if blitz:
        query = query.filter(func.lower(Football.blitz) == func.lower(blitz))
        
    if down:
        query = query.filter(Football.dn == down)
        
    if distance:
        query = query.filter(func.lower(Football.distance) == func.lower(distance))
        
    if field_position:
        query = query.filter(func.lower(Football.field_position) == func.lower(field_position))
    
    
    results = query.all()
    
   # filtered_df = pd.read_sql(results[1], session.connection())
    pretty_results = schema.dump(results)
    
    return pretty_results


#function to analyze blitz stats from filtered data

def blitz_stats(df):
    #count plays
    count_of_plays = df['play_no'].count()
    perc_of_total_plays = round(count_of_plays/df["play_no"].count()*100, 2)
    
    
    #adding iterator counter for number of unique results
    blitz_result = {}
    blitz_list=[]
    
    #looping through list of values to count occurences and add 
    #to empty dictionary
     
    # Select column contents by column
    # name using [] operator
    results = df["blitz"].values
    
    for item in results: 
        if (item in blitz_result): 
            blitz_result[item] += 1
        else: 
            blitz_result[item] = 1
            
    #loop through dictionary of counts to calculate percentage of total
    
    for key, value in blitz_result.items():
    
        #empty dictionary for next loop
        blitz_dict = {}

        #add key value pairs to new dictionary
        blitz_dict['Blitz Type'] = key
        blitz_dict['count'] = value
        blitz_dict['%_of_plays'] = round(value/count_of_plays * 100, 2)

        #append dic to list before looping
        blitz_list.append(blitz_dict)
    

    
    return blitz_list


#analyze stunt data from filtered list
def stunt_stats(df):
    #count plays
    count_of_plays = df['play_no'].count()
    
    #adding iterator counter for number of unique results
    stunt_result = {}
    stunt_list = []
    
    #looping through list of values to count occurences and add 
    #to empty dictionary
     
    # Select column contents by column
    # name using [] operator
    stunt = df["stunt"].values
    
    for item in stunt:
        if (item in stunt_result):
            stunt_result[item] += 1
        
        else:
            stunt_result[item] = 1
    
    #loop through dictionary of counts to calculate percentage of total
    for key, value in stunt_result.items():
        
        #empty dictionary for next loop
        stunt_dict = {}
        
        #add key value pairs to new dictionary
        stunt_dict['stunt_name'] = key
        stunt_dict['count'] = value
        stunt_dict['%_of_plays'] = round(value/count_of_plays * 100, 2)
        
        #append dic to list before looping
        stunt_list.append(stunt_dict)
        
    return stunt_list

#funtion to analyze coverage stats from filtered data
def coverage_stats(df):
    #count plays
    count_of_plays = df['play_no'].count()
    
    #adding iterator counter for number of unique results
    coverage_result = {}
    coverage_list = []
    
    #looping through list of values to count occurences and add 
    #to empty dictionary
     
    # Select column contents by column
    # name using [] operator
    coverage = df["coverage"].values
    
    for item in coverage:
        if (item in coverage_result):
            coverage_result[item] += 1
        
        else:
            coverage_result[item] = 1
    
    #loop through dictionary of counts to calculate percentage of total
    for key, value in coverage_result.items():
        
        #empty dictionary for next loop
        coverage_dict = {}
        
        #add key value pairs to new dictionary
        coverage_dict['coverage_name'] = key
        coverage_dict['count'] = value
        coverage_dict['%_of_plays'] = round(value/count_of_plays * 100, 2)
        
        #append dic to list before looping
        coverage_list.append(coverage_dict)
        
    return coverage_list
        

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

        data = pd.DataFrame(filter_function(school, start_date, end_date, defensive_front, blitz, down, distance, field_position))

        blitz_data = pd.DataFrame(blitz_stats(data))
        stunt_data = pd.DataFrame(stunt_stats(data))
        coverage_data = pd.DataFrame(coverage_stats(data))


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