import numpy as np
from flask import Flask, request,render_template
import pickle


app = Flask(__name__)
model = pickle.load(open('model.pkl', 'rb'))



@app.route('/')
def home():
    return render_template('index.html')



@app.route('/predict',methods=['POST'])
def predict():
    '''
    For rendering results on HTML GUI
    '''

    float_features = [float(x) for x in request.form.values()]
    
    prediction = model.predict([np.array(float_features)])

    output = "Male" if prediction[0] == 1 else "Female"

    return render_template('index.html', prediction_text='Gender is '+ output)



if __name__ == "__main__":
    app.run(debug=True)