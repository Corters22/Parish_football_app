import pandas as pd
from app import session, Football, schema, db
from sqlalchemy import func, create_engine
from sqlalchemy.orm import sessionmaker

uri = 'postgres://zimnwcywuosrtt:ff518e8bd538f2c8e5663077bcfd32bbba8488cad00e0495f2642819983a5443@ec2-23-23-182-238.compute-1.amazonaws.com:5432/dcbf9qrp2js5li'
engine = create_engine(uri)
session = sessionmaker(bind=engine)

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
    from sqlalchemy import create_engine
 
    
    conn = engine.connect()
    
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
    query = session.query(Football).filter(Football.qtr.is_(None)).delete()
    
    session.commit()
    #session.close()


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
        