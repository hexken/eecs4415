"""
app.py
----------
    Python portion of the dashboard app.
    To be executed on localhost of the Spark app's container.
    Run with
        python app.py


    Made for: EECS 4415 Big Data Systems Assignment #3, Part A
    Modified by: ken Tjhia
    Based on: https://www.toptal.com/apache/apache-spark-streaming-twitter
    Original author: Hanee' Medhat

"""
from flask import Flask, jsonify, request
from flask import render_template
import ast

app = Flask(__name__)
labels = []
values = []


@app.route("/")
def get_chart_page():
    global labels, values
    labels = []
    values = []
    return render_template('chart.html', values=values, labels=labels)


@app.route('/refreshData')
def refresh_graph_data():
    global labels, values
    print("labels now: " + str(labels))
    print("data now: " + str(values))
    return jsonify(sLabel=labels, sData=values)


@app.route('/updateData', methods=['POST'])
def update_data():
    global labels, values
    if not request.form or 'data' not in request.form:
        return "error", 400
    labels = ast.literal_eval(request.form['label'])
    values = ast.literal_eval(request.form['data'])
    print("labels received: " + str(labels))
    print("data received: " + str(values))
    return "success", 201


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5001)
