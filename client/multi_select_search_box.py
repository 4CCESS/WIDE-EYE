# multi_select_search_box.py

"""
MultiSelectSearchBox: a combined QLineEdit and popup QListWidget that
allows the user to search through a list of options and select multiple
entries via checkboxes. Includes “[Select All]” and “[Clear Selection]”
special commands and live filtering as the user types.
"""

from PySide6.QtWidgets import (
    QWidget, QLineEdit, QListWidget, QListWidgetItem, QVBoxLayout, QApplication
)
from PySide6.QtCore import Qt, QPoint, QEvent


class MultiSelectSearchBox(QWidget):
    """
    Widget providing a searchable, multi-select dropdown.

    Consists of a QLineEdit which, when clicked, pops up a QListWidget
    beneath it. The list contains checkable items plus two special
    commands: “[Select All]” and “[Clear Selection]”. As the user types
    into the line edit, the list is filtered in real time. Selections
    are reflected back in the line edit as comma-separated text.
    """

    def __init__(self, parent=None, placeholder="Type to filter..."):
        """
        Initialize the multi-select search box.

        Args:
            parent (QWidget, optional): The parent widget. Defaults to None.
            placeholder (str): Placeholder text shown in the line edit
                when no items are selected.
        """
        super().__init__(parent)

        # Line edit for typing and showing selections
        self.line_edit = QLineEdit()
        self.line_edit.setPlaceholderText(placeholder)

        # Popup list for searchable, checkable options
        self.list_widget = QListWidget()
        self.list_widget.setWindowFlags(Qt.Popup)
        self.list_widget.setFocusPolicy(Qt.NoFocus)
        self.list_widget.itemChanged.connect(self.update_text)

        # Store placeholder and items
        self.placeholder_text = placeholder
        self.all_items = []
        self.special_items = ["[Select All]", "[Clear Selection]"]

        # Prevent recursive updates when programmatically checking items
        self._ignore_update = False

        # Layout: line edit only
        layout = QVBoxLayout(self)
        layout.addWidget(self.line_edit)
        layout.setContentsMargins(0, 0, 0, 0)
        self.setLayout(layout)

        # Connect signals for filtering and showing the list
        self.line_edit.textEdited.connect(self.filter_list)
        self.line_edit.installEventFilter(self)

    def eventFilter(self, source, event):
        """
        Intercept mouse presses to show or hide the popup list.

        - Clicking on the line edit shows the list.
        - Clicking anywhere outside the list hides it.

        Args:
            source (QObject): The object where the event originated.
            event (QEvent): The event to filter.

        Returns:
            bool: True if the event is consumed, otherwise calls superclass.
        """
        if event.type() == QEvent.MouseButtonPress:
            if source is self.line_edit:
                self.show_list()
                return True
            elif self.list_widget.isVisible():
                # Click outside the popup hides it
                if not self.list_widget.geometry().contains(event.globalPos()):
                    self.list_widget.hide()
                    QApplication.instance().removeEventFilter(self)
                    return True
        return super().eventFilter(source, event)

    def show_list(self):
        """
        Display the popup list just below the line edit,
        adjusting its size to fit contents.
        """
        if not self.list_widget.isVisible():
            # Position below the line edit
            self.list_widget.move(self.mapToGlobal(QPoint(0, self.height())))
            self.adjust_list_size()
            self.list_widget.show()
            QApplication.instance().installEventFilter(self)

    def adjust_list_size(self):
        """
        Resize the popup list based on the number of items and
        a maximum visible count, ensuring a reasonable height.
        """
        count = self.list_widget.count()
        if count == 0:
            # Default minimum height
            height = 100
        else:
            item_height = self.list_widget.sizeHintForRow(0)
            max_visible = 8
            height = min(count, max_visible) * item_height + 8
        # Match width and update height
        self.list_widget.resize(self.width(), height)

    def set_items(self, items):
        """
        Populate the dropdown with a new set of items.

        Prepends the two special commands and sorts the remainder.

        Args:
            items (List[str]): The list of selectable item texts.
        """
        self.all_items = self.special_items + sorted(items)
        self.refresh_list()

    def refresh_list(self):
        """
        Clear and rebuild the QListWidget contents based on self.all_items.

        Each entry is made checkable and set to an unchecked state.
        """
        self.list_widget.clear()
        for text in self.all_items:
            item = QListWidgetItem(text)
            item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
            item.setCheckState(Qt.Unchecked)
            self.list_widget.addItem(item)

    def filter_list(self, text):
        """
        Hide or show items based on whether they contain the filter text.

        Special items (“[Select All]” and “[Clear Selection]”) are
        always shown regardless of the filter.

        Args:
            text (str): The current contents of the line edit.
        """
        lower = text.lower()
        for i in range(self.list_widget.count()):
            item = self.list_widget.item(i)
            if item.text() in self.special_items:
                item.setHidden(False)
            else:
                item.setHidden(lower not in item.text().lower())

    def update_text(self):
        """
        Synchronize the line edit’s text with the checked items.

        - If “[Select All]” is checked, all non-special items become checked.
        - If “[Clear Selection]” is checked, all items become unchecked.
        - Otherwise, the line edit displays checked items joined by commas,
          or reverts to the placeholder if none are checked.
        """
        if self._ignore_update:
            return

        # Handle special commands first
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

        # Collect regular selections
        selected = []
        for i in range(self.list_widget.count()):
            item = self.list_widget.item(i)
            if (item.checkState() == Qt.Checked and
                item.text() not in self.special_items):
                selected.append(item.text())

        # Update line edit
        if selected:
            self.line_edit.setText(", ".join(selected))
            self.line_edit.setPlaceholderText("")
        else:
            self.line_edit.setText("")
            self.line_edit.setPlaceholderText(self.placeholder_text)

    def selected_items(self):
        """
        Retrieve the list of non-special items currently checked.

        Returns:
            List[str]: Checked item texts, excluding the special commands.
        """
        result = []
        for i in range(self.list_widget.count()):
            item = self.list_widget.item(i)
            if (item.checkState() == Qt.Checked and
                item.text() not in self.special_items):
                result.append(item.text())
        return result

    def select_all_items(self):
        """
        Check all non-special items, leaving special commands unchecked,
        and update the line edit to reflect the new selections.
        """
        for i in range(self.list_widget.count()):
            item = self.list_widget.item(i)
            if item.text() in self.special_items:
                item.setCheckState(Qt.Unchecked)
            else:
                item.setCheckState(Qt.Checked)
        self.update_text()

    def clear_selection(self):
        """
        Uncheck every item and reset the line edit to show its placeholder.
        """
        for i in range(self.list_widget.count()):
            item = self.list_widget.item(i)
            item.setCheckState(Qt.Unchecked)
        self.update_text()
