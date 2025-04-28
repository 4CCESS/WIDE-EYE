# multi_select_search_box.py

from PySide6.QtWidgets import QWidget, QLineEdit, QListWidget, QListWidgetItem, QVBoxLayout, QApplication
from PySide6.QtCore import Qt, QPoint, QEvent


class MultiSelectSearchBox(QWidget):
    def __init__(self, parent=None, placeholder="Type to filter..."):
        super().__init__(parent)

        self.line_edit = QLineEdit()
        self.list_widget = QListWidget()
        self.list_widget.setWindowFlags(Qt.Popup)
        self.list_widget.setFocusPolicy(Qt.NoFocus)
        self.list_widget.itemChanged.connect(self.update_text)

        self.placeholder_text = placeholder
        self.line_edit.setPlaceholderText(self.placeholder_text)

        layout = QVBoxLayout(self)
        layout.addWidget(self.line_edit)
        layout.setContentsMargins(0, 0, 0, 0)
        self.setLayout(layout)

        self.line_edit.textEdited.connect(self.filter_list)
        self.line_edit.installEventFilter(self)

        self.all_items = []
        self.special_items = ["[Select All]", "[Clear Selection]"]

        self._ignore_update = False  # Important for preventing recursion during bulk operations

    def eventFilter(self, source, event):
        if event.type() == QEvent.MouseButtonPress:
            if source == self.line_edit:
                self.show_list()
            else:
                if self.list_widget.isVisible():
                    click_pos = event.globalPos()
                    if not self.list_widget.geometry().contains(click_pos):
                        self.list_widget.hide()
                        QApplication.instance().removeEventFilter(self)
        return super().eventFilter(source, event)

    def show_list(self):
        if not self.list_widget.isVisible():
            self.list_widget.move(self.mapToGlobal(QPoint(0, self.height())))
            self.adjust_list_size()
            self.list_widget.show()
            QApplication.instance().installEventFilter(self)

    def adjust_list_size(self):
        count = self.list_widget.count()
        if count == 0:
            height = 100  # Minimum height
        else:
            item_height = self.list_widget.sizeHintForRow(0)
            max_visible_items = 8
            height = min(count, max_visible_items) * item_height + 8
        self.list_widget.resize(self.width(), height)

    def set_items(self, items):
        self.all_items = self.special_items + sorted(items)
        self.refresh_list()

    def refresh_list(self):
        self.list_widget.clear()
        for text in self.all_items:
            item = QListWidgetItem(text)
            item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
            item.setCheckState(Qt.Unchecked)
            self.list_widget.addItem(item)

    def filter_list(self, text):
        lower_text = text.lower()
        for i in range(self.list_widget.count()):
            item = self.list_widget.item(i)
            if item.text() in self.special_items:
                item.setHidden(False)  # Always show special options
            else:
                item.setHidden(lower_text not in item.text().lower())

    def update_text(self):
        if self._ignore_update:
            return

        selected = []
        for i in range(self.list_widget.count()):
            item = self.list_widget.item(i)
            if item.checkState() == Qt.Checked:
                if item.text() == "[Select All]":
                    self._ignore_update = True
                    self.select_all_items()
                    self._ignore_update = False
                    return
                elif item.text() == "[Clear Selection]":
                    self._ignore_update = True
                    self.clear_selection()
                    self._ignore_update = False
                    return

        for i in range(self.list_widget.count()):
            item = self.list_widget.item(i)
            if item.checkState() == Qt.Checked and item.text() not in self.special_items:
                selected.append(item.text())

        if selected:
            self.line_edit.setText(", ".join(selected))
            self.line_edit.setPlaceholderText("")
        else:
            self.line_edit.setText("")
            self.line_edit.setPlaceholderText(self.placeholder_text)

    def selected_items(self):
        selected = []
        for i in range(self.list_widget.count()):
            item = self.list_widget.item(i)
            if item.checkState() == Qt.Checked and item.text() not in self.special_items:
                selected.append(item.text())
        return selected

    def select_all_items(self):
        for i in range(self.list_widget.count()):
            item = self.list_widget.item(i)
            if item.text() not in self.special_items:
                item.setCheckState(Qt.Checked)
            else:
                item.setCheckState(Qt.Unchecked)  # Important: Uncheck "[Select All]" after applying
        self.update_text()

    def clear_selection(self):
        for i in range(self.list_widget.count()):
            item = self.list_widget.item(i)
            item.setCheckState(Qt.Unchecked)
        self.update_text()
