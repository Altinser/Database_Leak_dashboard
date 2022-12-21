import sys
import traceback
from PyQt5.QtWidgets import QFrame, QSizePolicy, QSpacerItem, QTableWidgetItem, QFileDialog, QComboBox, QMessageBox, QCheckBox, QTableWidget, QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLineEdit, QLabel
from PyQt5.QtGui import QFont, QColor, QStandardItem
from PyQt5.QtCore import QThread, pyqtSignal, Qt, QSize
from PyQt5.Qsci import QsciScintilla, QsciLexerSQL
import threading
import sqlite3
import os
import random
import re

def excepthook(exc_type, exc_value, exc_tb):
    tb = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
    print("error catched!:")
    print("error message:\n", tb)
    QApplication.quit()

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("My Data Base Dashboard")
        self.resize(700, 500)
        self.central_widget = QWidget()
        self.setCentralWidget(self.central_widget)
        self.main_layout = QVBoxLayout()
        self.central_widget.setLayout(self.main_layout)
        self.input_widget = QsciScintilla()
        self.input_widget.setFixedHeight(int(self.height() * 0.3))
        self.input_widget.setFont(QFont("Courier"))
        self.output_table = QTableWidget()
        self.output_table.setFixedHeight(int(self.height() * 0.5))

        self.lexer = QsciLexerSQL()
        self.input_widget.setLexer(self.lexer)
        self.validate_button = QPushButton("Valider")
        self.validate_button.clicked.connect(self.validate_button_clicked)
        self.input_widget_layout = QHBoxLayout()
        self.input_widget_layout.addWidget(self.input_widget)
        self.input_widget_layout.addWidget(self.validate_button)
        self.add_data_button = QPushButton("ADD Data")
        self.add_data_button.clicked.connect(self.add_data_button_clicked)
        self.rainbow_table_button = QPushButton("Rainbow Table")
        self.trie_data_button = QPushButton("Trie Data")
        self.db_info_text = QLabel("DB information")
        self.right_layout = QHBoxLayout()
        self.right_layout.addWidget(self.add_data_button)
        self.right_layout.addWidget(self.rainbow_table_button)
        self.right_layout.addWidget(self.trie_data_button)
        self.right_layout.addWidget(self.db_info_text)
        self.main_layout.addLayout(self.input_widget_layout)
        self.main_layout.addWidget(self.output_table)
        self.main_layout.addLayout(self.right_layout)
        self.connection_thread = ConnectionThread(self)
        self.connection_thread.start()

    def validate_button_clicked(self):
        query = self.input_widget.text()
        confirm_message = "<p align='center'>Voulez-vous vraiment exécuter la requête ?</p>"
        confirm = QMessageBox()
        confirm.setTextFormat(Qt.RichText)
        confirm.setText(confirm_message)
        confirm.setWindowTitle("Confirmation")
        confirm.setStandardButtons(QMessageBox.Cancel | QMessageBox.Ok)
        res = confirm.exec_()
        if res == QMessageBox.Ok:

            result, cursor = self.connection_thread.execute_query(query)

            if result == "error":
                QMessageBox.critical(self, "Erreur", "La requête est invalide:\n" + cursor)
                return

            column_names = [description[0] for description in cursor.description]

            if isinstance(result, str):
                self.output_table.setRowCount(0)
                self.output_table.setColumnCount(1)
                self.output_table.setHorizontalHeaderLabels(["Résultat"])
                self.output_table.setItem(0, 0, QTableWidgetItem(result))
                self.output_table.resizeColumnsToContents()
                return

            self.output_table.setColumnCount(len(column_names))
            self.output_table.setRowCount(len(result))
            self.output_table.setHorizontalHeaderLabels(column_names)

            for i, row in enumerate(result):
                for j, cell in enumerate(row):
                    self.output_table.setItem(i, j, QTableWidgetItem(str(cell)))
            self.output_table.resizeColumnsToContents()

    def add_data_button_clicked(self):
        self.add_data_window = AddDataWindow(self.connection_thread)
        self.add_data_window.show()

class AddDataWindow(QMainWindow):
    def __init__(self, connection_thread):
        super().__init__()
        self.connection_thread = connection_thread
        self.setWindowTitle("Ajouter des données")
        self.resize(450, 400)
        self.central_widget = QWidget()
        self.setCentralWidget(self.central_widget)
        self.button_layout = QHBoxLayout()
        self.file_path_layout = QHBoxLayout()
        self.separator_title = QLabel("Séparateur :")
        self.output_table_title = QLabel("Donnée parser :")
        self.table_selection_title = QLabel("Tables à alimenter :")
        line = QFrame()
        line.setFrameShape(QFrame.HLine)
        self.table_selection_layout = QVBoxLayout()
        self.main_layout = QVBoxLayout()
        self.central_widget.setLayout(self.main_layout)
        self.file_path_input = QLineEdit()
        self.file_path_input.setPlaceholderText("Chemin data base ...")
        separator_input = QLineEdit()
        separator_input.textChanged.connect(self.import_data)
        separator_input.setMinimumSize(QSize(20, 20))
        self.choose_file_button = QPushButton("Choisir un fichier")
        self.choose_file_button.clicked.connect(self.choose_file)
        self.cancel_button = QPushButton("Annuler")
        self.cancel_button.clicked.connect(self.close)
        self.find_separator_button = QPushButton("Trouver un séparateur")
        self.find_separator_button.clicked.connect(self.find_separators)
        self.import_data_button = QPushButton("Importer les données")
        self.import_data_button.setEnabled(False)
        self.import_data_button.clicked.connect(self.import_data)
        self.data_table = QTableWidget()
        self.table_checkboxes = []
        self.table_dropdowns = []
        tables = self.connection_thread.get_tables()
        self.table_selection_layout.addWidget(self.table_selection_title)
        for table in tables:
            checkbox = QCheckBox(table)
            checkbox.clicked.connect(self.table_checkbox_clicked)
            self.table_checkboxes.append(checkbox)
            dropdown = QComboBox()
            dropdown.setSizeAdjustPolicy(QComboBox.AdjustToMinimumContentsLength)
            self.table_dropdowns.append(dropdown)

        self.button_layout.addWidget(self.cancel_button)
        self.button_layout.addWidget(self.import_data_button)
        self.file_path_layout.addWidget(self.file_path_input)
        self.file_path_layout.addWidget(self.choose_file_button)
        self.main_layout.addLayout(self.file_path_layout)
        self.main_layout.addSpacerItem(QSpacerItem(0, 10))
        self.add_separator_button = QPushButton("+")
        self.add_separator_button.setSizePolicy(QSizePolicy.Minimum, QSizePolicy.Preferred)
        self.add_separator_button.clicked.connect(self.add_separator_input)
        self.separator_layout = QHBoxLayout()
        self.main_separator_layout = QHBoxLayout()
        self.main_layout.addSpacerItem(QSpacerItem(0, 10))
        self.separator_layout.addWidget(separator_input)
        self.separator_layout.setAlignment(Qt.AlignRight)
        self.main_separator_layout.addLayout(self.separator_layout)
        self.main_separator_layout.addWidget(self.add_separator_button)
        self.main_layout.addWidget(self.separator_title)
        self.main_layout.addLayout(self.main_separator_layout)
        self.main_layout.addWidget(self.find_separator_button)
        self.main_layout.addSpacerItem(QSpacerItem(0, 10))
        self.main_layout.addWidget(self.output_table_title)
        self.main_layout.addWidget(self.data_table)
        self.main_layout.addSpacerItem(QSpacerItem(0, 10))
        self.main_layout.addWidget(self.table_selection_title)

        for dropdown in self.table_dropdowns:
            dropdown.currentIndexChanged.connect(self.dropdown_selection_changed)

        for i in range(len(tables)):
            table_layout = QHBoxLayout()
            table_layout.addWidget(self.table_checkboxes[i])
            table_layout.addWidget(self.table_dropdowns[i])
            self.main_layout.addLayout(table_layout)

        self.main_layout.addSpacerItem(QSpacerItem(0, 10))
        self.main_layout.addLayout(self.button_layout)

    def dropdown_selection_changed(self):
        # Get the index of the dropdown that triggered the signal
        sender_index = self.table_dropdowns.index(self.sender())
        # Get the selected column number of the dropdown
        selected_column = self.table_dropdowns[sender_index].currentIndex()
        # Check if the selected column number is being used by another dropdown
        for i, dropdown in enumerate(self.table_dropdowns):
            if i != sender_index and dropdown.currentIndex() == selected_column and dropdown.currentIndex() != -1:
                # Display an error message or set the selected index of the dropdown to an unused column number
                QMessageBox.warning(self, "Error", "Ce nombre de colonne est déjà pris")
                self.table_dropdowns[sender_index].setCurrentIndex(-1)
                return


    def add_separator_input(self):
        separator_input = QLineEdit()
        separator_input.setMinimumSize(QSize(20, 20))
        separator_input.textChanged.connect(self.import_data)
        self.separator_layout.addWidget(separator_input)

    def choose_file(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "Choisir un fichier")
        self.file_path_input.setText(file_path)
        if any(checkbox.isChecked() for checkbox in self.table_checkboxes):
            self.import_data_button.setEnabled(True)
        else:
            self.import_data_button.setEnabled(False)

    def choose_random_lines(self):
        # Read the first 20 lines of the file
        with open(self.file_path_input.text(), 'r') as f:
            lines = [next(f).strip() for _ in range(20)]

        # Choose 3 random lines
        random_indices = random.sample(range(20), 3)
        random_lines = [lines[i] for i in random_indices]
        return random_lines

    def parse_data(self, lines):
        separator = []

        for i in range (0,self.separator_layout.count()):
            if self.separator_layout.itemAt(i).widget().text() != "":
                separator.append(str(self.separator_layout.itemAt(i).widget().text()))

        pattern = '|'.join(map(re.escape, separator))

        self.data_table.setColumnCount(len(separator)+1)
        for line in lines:
            if len(separator) > 0 or separator != "":
                values = re.split(pattern,line)
            else:
                values = [line]
            row = self.data_table.rowCount()
            self.data_table.insertRow(row)

            for col, value in enumerate(values):
                item = QTableWidgetItem(value)
                self.data_table.setItem(row, col, item)

        column_count = self.data_table.columnCount()
        for i, checkbox in enumerate(self.table_dropdowns):
            dropdown = self.table_dropdowns[i]
            dropdown.clear()  # clear the dropdown of any previous items
            for j in range(column_count):
                dropdown.addItem(str(j+1))  # add the column number as a string to the dropdown
            dropdown.setCurrentIndex(-1)

        self.data_table.resizeColumnsToContents()

    def import_data(self):

        self.data_table.setRowCount(0)
        self.data_table.setColumnCount(0)

        random_lines = self.choose_random_lines()
        self.parse_data(random_lines)



    def find_separators(self):
        file_path = self.file_path_input.text()
        separators = [',', ';', '\t', '|', ':', '#', '!', '^']
        counts = {}
        for separator in separators:
            counts[separator] = 0

        if os.path.isfile(file_path):
            with open(file_path, "r") as file:
                lines = file.readlines()[:10000]
                for line in lines:
                    for separator in separators:
                        counts[separator] += line.count(separator)
            total_count = sum(counts.values())
            if total_count == 0:
                return None
            cpt = 0
            for separator, count in counts.items():
                if count / total_count > 0.05:
                    cpt = cpt + 1
                    if cpt <= self.separator_layout.count() :
                        self.separator_layout.itemAt(cpt-1).widget().setText(separator)
                    else:
                        separator_input = QLineEdit()
                        separator_input.setMinimumSize(QSize(20, 20))
                        separator_input.setText(separator)
                        separator_input.textChanged.connect(self.import_data)
                        self.separator_layout.addWidget(separator_input)
        self.import_data()

    def table_checkbox_clicked(self):
        if self.file_path_input.text() and any(checkbox.isChecked() for checkbox in self.table_checkboxes):
            self.import_data_button.setEnabled(True)
        else:
            self.import_data_button.setEnabled(False)

class ConnectionThread(QThread):
    def __init__(self,main_window):
        super().__init__()
        self.main_window = main_window
        self.connection = sqlite3.connect("D:\ALL_1.db", check_same_thread=False)
        self.cursor = self.connection.cursor()

    def run(self):
        tables = self.get_tables()
        db_info_text = "Taille de la base de données : {} Go\n".format(format(os.stat('D:\ALL_1.db').st_size / 1000000000, '.2f'))
        for i in range(len(tables)):
            db_info_text += "{}\n".format(tables[i])
        self.main_window.db_info_text.setText(db_info_text)

    def get_column_names(self, table_name):
        query = f"PRAGMA table_info({table_name})"
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        return [row[1] for row in result]

    def execute_query(self, query):
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            return result, cursor
        except Exception as e:
            return "error", str(e)

    def get_tables(self):
        query = "SELECT name FROM sqlite_master WHERE type='table'"
        self.cursor.execute(query)
        tables = [table[0] for table in self.cursor.fetchall()]
        return tables

if __name__ == '__main__':
    sys.excepthook = excepthook
    app = QApplication(sys.argv)
    main_window = MainWindow()
    main_window.show()
    sys.exit(app.exec_())

