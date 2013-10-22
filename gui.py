#!/usr/bin/env python

from gi.repository import Gtk
from gi.repository import AppIndicator3 as appindicator
import sys


class MainWindow(Gtk.ApplicationWindow):

    def __init__(self, app):
        super(MainWindow, self).__init__(title='Cronotrigger', application=app)
        self.set_default_size(320, 240)
        self.connect('delete-event', lambda w, e: w.hide() or True)

        # icon = self.render_icon(Gtk.STOCK_DIALOG_INFO, Gtk.ICON_SIZE_BUTTON)
        # self.set_icon(icon)

        label = Gtk.Label()
        label.set_text('Hello World!')
        self.add(label)


class Application(Gtk.Application):

    def __menu_activated_configure(self, menu_item):
        self.__window.show_all()

    def __menu_activated_quit(self, menu_item):
        self.quit()

    def __setup_indicator(self):
        # Possible values are:
        # APPLICATION_STATUS, COMMUNICATIONS, HARDWARE, OTHER, SYSTEM_SERVICES
        # http://developer.ubuntu.com/resources/technologies/application-indicators/
        indicator = appindicator.Indicator.new(
            'cronotrigger-gui',
            'indicator-messages',  # icon name
            # appindicator.IndicatorCategory.APPLICATION_STATUS,
            appindicator.IndicatorCategory.SYSTEM_SERVICES,
        )
        # Possible values are:
        # ACTIVE, PASSIVE, ATTENTION
        indicator.set_status(appindicator.IndicatorStatus.ACTIVE)
        indicator.set_attention_icon('indicator-messages-new')  # icon name

        menu = Gtk.Menu()

        name = 'Configure...'
        menu_item = Gtk.MenuItem(name)
        menu.append(menu_item)
        menu_item.connect('activate', self.__menu_activated_configure)
        menu_item.show()

        name = 'Quit'
        menu_item = Gtk.MenuItem(name)
        menu.append(menu_item)
        menu_item.connect('activate', self.__menu_activated_quit)
        menu_item.show()

        indicator.set_menu(menu)
        self.__indicator = indicator

    def __init__(self):
        super(Application, self).__init__()

    def do_activate(self):
        self.__setup_indicator()
        self.__window = MainWindow(self)
        self.__window.show_all()  # TODO REMOVE ME

    def do_startup(self):
        Gtk.Application.do_startup(self)


if __name__ == '__main__':
    app = Application()
    exit_status = app.run(sys.argv)
    sys.exit(exit_status)
