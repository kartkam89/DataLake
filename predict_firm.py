from keras.models import load_model
import pickle
import nltk
from nltk.stem import WordNetLemmatizer
import numpy as np
import pandas as pd


nltk.download('wordnet')
wnl = WordNetLemmatizer()


class predict_firm:
    def __init__(self,input):
        self.raw_input = input
        self.processing()


    def processing(self):
        self.load_model()
        self.clean_records()
        self.sent_to_words()
        self.lemming()
        self.vectorise()
        self.prediction()
        return self

    def load_model(self):
        self.model = load_model("c:\\garbage\\model.h5")
        self.model_words_labels = open("c:\\garbage\\model_items.pkl", "rb")
        self.model_words_labels = pickle.load(self.model_words_labels)
        self.words_used = self.model_words_labels[0]
        self.words_list = self.model_words_labels[1]
        return self

    def clean_records(self):
        self.cleaned_input = self.raw_input.replace("Other / TBD"," ")\
                                            .replace(":"," ").replace("("," ")\
                                            .replace("&"," ").replace("/"," ")\
                                            .replace(")"," ").replace(","," ")\
                                            .replace("+"," ").replace("-"," ")\
                                            .replace("?"," ").replace("#"," ")\
                                            .replace("nan"," ").replace("’s","")\
                                            .replace('™',"").replace('®',"")
        return self

    def sent_to_words(self):
        self.cleaned_input = self.cleaned_input.lower().split()
        return self

    def lemming(self):
        self.cleaned_input = [wnl.lemmatize(i) for i in self.cleaned_input]
        return self

    def vectorise(self):
        self.vectored = np.zeros((1,len(self.words_used)))
        for i in self.cleaned_input:
            if i in self.words_used:
                self.vectored[0,self.words_used.index(i)] = 1.
            else:
                pass

        self.vectored = pd.DataFrame(self.vectored)[self.words_list]
        return self

    def prediction(self):
        self.output = self.model.predict(self.vectored)
        self.output = self.model_words_labels[3][np.argmax(self.output)]

        return self






