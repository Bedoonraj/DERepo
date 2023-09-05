from flask import Flask,render_template,request
import pandas as pd
from kafka import KafkaProducer
import json

movie=pd.read_csv('./data/movies.csv')
movie_list=movie['title'].drop_duplicates().tolist()
rating=[0.5,1,1.5,2,2.5,3,3.5,4,4.5,5]
app=Flask(__name__)

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8'))

@app.route('/',methods=['GET'])
def Home():
    user_id=request.args.get('username')
    movie_name=request.args.get('favoritemovie')
    rating_val=request.args.get('rating_val')
    # send rating data to Kafka
    if user_id !=None:
        rating_data = {'user_id': user_id, 'movie_name': movie_name, 'rating_val': rating_val}
        producer.send('movierating', value=rating_data)
    return render_template('index.html',movies=movie_list,rating=rating)

app.run(debug=True)


