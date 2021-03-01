import PySimpleGUI as sg
import subprocess
# Define the window's contents
layout = [[sg.Text("What would you like to know?")],
          [sg.Input(key='-INPUT-')],
          [sg.Text(size=(150, 10), key='-OUTPUT-')],
          [sg.Button('Ok'), sg.Button('Quit')]]

# Create the window
window = sg.Window('Question Interface', layout, no_titlebar=True)

# Display and interact with the Window using an Event Loop
while True:
    event, values = window.read()
    # See if user wants to quit or window was closed
    if event == sg.WINDOW_CLOSED or event == 'Quit':
        break
    # Output a message to the window

    output = subprocess.check_output(["python", "/Users/mlcb/PycharmProjects/Thesis/thesis_py/entity_linking.py",
                                      values['-INPUT-']])

    window['-OUTPUT-'].update(output.decode("utf-8"))

# Finish up by removing from the screen
window.close()
