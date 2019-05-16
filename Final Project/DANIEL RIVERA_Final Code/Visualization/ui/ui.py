from tkinter import Tk, Label, Button
from PIL import ImageTk, Image
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import matplotlib.figure
import matplotlib.patches
import sys
import json
import os
import time

f = open("results.txt", "r")
lines = f.readlines()

age_data = json.loads(lines[0])
gender_data = json.loads(lines[1])
person_data = json.loads(lines[2])
tweets = list(map(lambda v: v[:-1], lines[3:]))

labels_age = '15 to 24', '25 to 34', '35 to 44', '45 to 54'
sizes_age = age_data['probability']['values']
max_age = int(max(sizes_age) * 100)

labels_gender = 'Male', 'Female'
sizes_gender = gender_data['probability']['values']
max_gender = int(max(sizes_gender) * 100)

labels_person = 'Nervous', 'Secure', 'Friendly', 'Detached', 'Outgoing', 'Reserved', 'Organized', 'Careless', 'Inventive', 'Cautious'
sizes_person = person_data['probability']['values']
max_person = int(max(sizes_person) * 100)

def func(pct, max_val):
	if pct < max_val:
		return ""
	return "{:.0f}%".format(pct)

fig = matplotlib.figure.Figure(figsize=(30,5))

age = fig.add_subplot(131)
age.pie(sizes_age, autopct=lambda pct: func(pct, max_age))
age.legend(labels_age, ncol=2, bbox_to_anchor=[0.95, 0.1])
age.set_title('AGE')
circle_age = matplotlib.patches.Circle((0,0), 0.7, color='white')
age.add_artist(circle_age)

gender = fig.add_subplot(132)
gender.pie(sizes_gender, autopct=lambda pct: func(pct, max_gender))
gender.legend(labels_gender, loc='lower center', ncol=2)
gender.set_title('GENDER')
circle_gender = matplotlib.patches.Circle((0,0), 0.7, color='white')
gender.add_artist(circle_gender)

person = fig.add_subplot(133)
person.pie(sizes_person, autopct=lambda pct: func(pct, max_person))
person.legend(labels_person, ncol=2, bbox_to_anchor=[0.97, 0.1])
person.set_title('PERSONALITY')
circle_person = matplotlib.patches.Circle((0,0), 0.7, color='white')
person.add_artist(circle_person)

box_age = age.get_position()
age.set_position([box_age.x0, box_age.y0 + box_age.height * 0.1,
                 		box_age.width, box_age.height * 0.9])
box_gender = gender.get_position()
gender.set_position([box_gender.x0, box_gender.y0 + box_gender.height * 0.1,
                 		box_gender.width, box_gender.height * 0.9])
box_person = person.get_position()
person.set_position([box_person.x0, box_person.y0 + box_person.height * 0.1,
                 		box_person.width, box_person.height * 0.9])

url = "https://twitter.com/Twitter/status/{}"
def show_original():
	tweet = sys.argv[1]
	if tweet[0:5] != "https":
		tweet = url.format(tweet)
	os.system("xdg-open {} &".format(tweet))

def show_similar():
	os.system("xdg-open {} &".format(url.format(tweets[0])))
	time.sleep(1)
	for tweet in tweets[1:]:
		os.system("xdg-open {} &".format(url.format(tweet)))

window = Tk()
window.winfo_toplevel().title("Author Profiling on Twitter")
window.configure(background='white')
window.geometry('1200x640+0+0')

title = Label(window, text="Author Profiling On Twitter:\nPredicting Age, Gender and Personality Traits", font=("Helvetica", 28), bg='white')
title.pack()

original = Button(window, text="Original Tweet", command=show_original, width=20)
original.pack()

similar = Button(window, text="Top 5 Most Similar Tweets", command=show_similar, width=20)
similar.pack()

canvas = FigureCanvasTkAgg(fig, master=window)
canvas.get_tk_widget().pack()
canvas.draw()

window.mainloop()