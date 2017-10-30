import functools
import operator
import csv
import six
import codecs
import os.path
import re
import prettytable
from kql.column_guesser import ColumnGuesserMixin


def unduplicate_field_names(field_names):
    """Append a number to duplicate field names to make them unique. """
    res = []
    for k in field_names:
        if k in res:
            i = 1
            while k + '_' + str(i) in res:
                i += 1
            k += '_' + str(i)
        res.append(k)
    return res

class UnicodeWriter(object):
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """

    # Object constructor
    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = six.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        if six.PY2:
            _row = [s.encode("utf-8")
                    if hasattr(s, "encode")
                    else s
                    for s in row]
        else:
            _row = row
        self.writer.writerow(_row)
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        if six.PY2:
           data = data.decode("utf-8")
           # ... and reencode it into the target encoding
           data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)
        self.queue.seek(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

class CsvResultDescriptor(object):
    """Provides IPython Notebook-friendly output for the feedback after a ``.csv`` called."""

    # Object constructor
    def __init__(self, file_path):
        self.file_path = file_path

    # Printable unambiguous presentation of the object
    def __repr__(self):
        return 'CSV results at %s' % os.path.join(os.path.abspath('.'), self.file_path)

    # IPython html presentation of the object
    def _repr_html_(self):
        return '<a href="%s">CSV results</a>' % os.path.join('.', 'files', self.file_path)




def _nonbreaking_spaces(match_obj):
    """
    Make spaces visible in HTML by replacing all `` `` with ``&nbsp;``

    Call with a ``re`` match object.  Retain group 1, replace group 2
    with nonbreaking speaces.
    """
    spaces = '&nbsp;' * len(match_obj.group(2))
    return '%s%s' % (match_obj.group(1), spaces)

_cell_with_spaces_pattern = re.compile(r'(<td>)( {2,})')



class ResultSet(list, ColumnGuesserMixin):
    """
    Results of a query.

    Can access rows listwise, or by string value of leftmost column.
    """

    # Object constructor
    def __init__(self, queryResult, query, config):
        # list of keys
        self.keys = queryResult.keys()

        # query
        self.sql = query

        # configuration
        self.config = config

        # Automatically limit the size of the returned result sets
        self.limit = config.autolimit

        # table printing style to any of prettytable's defined styles (currently DEFAULT, MSWORD_FRIENDLY, PLAIN_COLUMNS, RANDOM)
        style_name = config.style
        self.style = prettytable.__dict__[style_name.upper()]

        self.show_chart = False
        if queryResult.returns_rows:
            if self.limit:
                list.__init__(self, queryResult.fetchmany(size=self.limit))
            else:
                list.__init__(self, queryResult.fetchall())

            self.field_names = unduplicate_field_names(self.keys)
            self.pretty = PrettyTable(self.field_names, style=self.style)
            # self.pretty.set_style(self.style)
            self.records_count = queryResult.recordscount()
            self.visualization = queryResult.extended_properties("Visualization")
            self.title = queryResult.extended_properties("Title")
        else:
            list.__init__(self, [])
            self.pretty = None


    # IPython html presentation of the object
    def _repr_html_(self):
        _cell_with_spaces_pattern = re.compile(r'(<td>)( {2,})')
        if self.pretty:
            self.pretty.add_rows(self)
            result = self.pretty.get_html_string()
            result = _cell_with_spaces_pattern.sub(_nonbreaking_spaces, result)
            if self.config.displaylimit and len(self) > self.config.displaylimit:
                result = '%s\n<span style="font-style:italic;text-align:center;">%d rows, truncated to displaylimit of %d</span>' % (
                    result, len(self), self.config.displaylimit)
            return result
        else:
            return None


    # Printable pretty presentation of the object
    def __str__(self, *arg, **kwarg):
        self.pretty.add_rows(self)
        return str(self.pretty or '')


    # For iterator self[key]
    def __getitem__(self, key):
        """
        Access by integer (row position within result set)
        or by string (value of leftmost column)
        """
        try:
            return list.__getitem__(self, key)
        except TypeError:
            result = [row for row in self if row[0] == key]
            if not result or len(result) == 0:
                raise KeyError(key)
            if len(result) > 1:
                raise KeyError('%d results for "%s"' % (len(result), key))
            return result[0]

    def dict(self):
        """Returns a single dict built from the result set
        Keys are column names; values are a tuple"""
        return dict(zip(self.keys, zip(*self)))


    def dicts(self):
        "Iterator yielding a dict for each row"
        for row in self:
            yield dict(zip(self.keys, row))


    def DataFrame(self):
        "Returns a Pandas DataFrame instance built from the result set."
        import pandas as pd
        frame = pd.DataFrame(self, columns=(self and self.keys) or [])
        return frame

    def visualization_chart(self):
        # https://kusto.azurewebsites.net/docs/queryLanguage/query_language_renderoperator.html
        if not self.visualization or self.visualization == 'table':
            return None
        self.show_chart = True
        # First column is color-axis, second column is numeric
        if self.visualization == 'piechart':
            chart = self.render_pie(" ", self.title)
        # First column is x-axis, and can be text, datetime or numeric. Other columns are numeric, displayed as horizontal strips.
        # kind = default, unstacked, stacked, stacked100 (Default, same as unstacked; unstacked - Each "area" to its own; stacked - "Areas" are stacked to the right; stacked100 - "Areas" are stacked to the right, and stretched to the same width)
        elif self.visualization == 'barchart':
            chart = self.render_barh(" ", self.title)
        # Like barchart, with vertical strips instead of horizontal strips.
        # kind = default, unstacked, stacked, stacked100 
        elif self.visualization == 'columnchart':
            chart = self.render_bar(" ", self.title)
        # Area graph. First column is x-axis, and should be a numeric column. Other numeric columns are y-axes.
        # kind = default, unstacked, stacked, stacked100 
        elif self.visualization == 'areachart':
            chart = self.render_areachart(" ", self.title)
        # Line graph. First column is x-axis, and should be a numeric column. Other numeric columns are y-axes.
        elif self.visualization == 'linechart':
            chart = self.render_linechart(" ", self.title)
        # Line graph. First column is x-axis, and should be datetime. Other columns are y-axes.
        elif self.visualization == 'timechart':
            chart = self.pie(" ", self.title)
        # Similar to timechart, but highlights anomalies using an external machine-learning service.
        elif self.visualization == 'anomalychart':
            chart = self.render_anomalychart(" ", self.title)
        # Stacked area graph. First column is x-axis, and should be a numeric column. Other numeric columns are y-axes.
        elif self.visualization == 'stackedareachart':
            chart = self.pie(" ", self.title)
        # Last two columns are the x-axis, other columns are y-axis.
        elif self.visualization == 'ladderchart':
            chart = self.pie(" ", self.title)
        # Interactive navigation over the events time-line (pivoting on time axis)
        elif self.visualization == 'timepivot':
            chart = self.pie(" ", self.title)
        # Displays a pivot table and chart. User can interactively select data, columns, rows and various chart types.
        elif self.visualization == 'pivotchart':
            chart = self.pie(" ", self.title)
        self.show_chart = False
        return chart



    def pie(self, key_word_sep=" ", title=None, **kwargs):
        """Generates a pylab pie chart from the result set.

        ``matplotlib`` must be installed, and in an
        IPython Notebook, inlining must be on::

            %%matplotlib inline

        Values (pie slice sizes) are taken from the
        rightmost column (numerical values required).
        All other columns are used to label the pie slices.

        Parameters
        ----------
        key_word_sep: string used to separate column values
                      from each other in pie labels
        title: Plot title, defaults to name of value column

        Any additional keyword arguments will be passsed
        through to ``matplotlib.pylab.pie``.
        """
        self.guess_pie_columns(xlabel_sep=key_word_sep)
        import matplotlib.pylab as plt
        pie = plt.pie(self.ys[0], labels=self.xlabels, **kwargs)
        plt.title(title or self.ys[0].name)
        if self.show_chart:
            plt.show()
        return pie

    def plot(self, title=None, **kwargs):
        """Generates a pylab plot from the result set.

        ``matplotlib`` must be installed, and in an
        IPython Notebook, inlining must be on::

            %%matplotlib inline

        The first and last columns are taken as the X and Y
        values.  Any columns between are ignored.

        Parameters
        ----------
        title: Plot title, defaults to names of Y value columns

        Any additional keyword arguments will be passsed
        through to ``matplotlib.pylab.plot``.
        """
        import matplotlib.pylab as plt
        self.guess_plot_columns()
        self.x = self.x or range(len(self.ys[0]))
        coords = functools.reduce(operator.add, [(self.x, y) for y in self.ys])
        plot = plt.plot(*coords, **kwargs)
        if hasattr(self.x, 'name'):
            plt.xlabel(self.x.name)
        ylabel = ", ".join(y.name for y in self.ys)
        plt.title(title or ylabel)
        plt.ylabel(ylabel)
        return plot

    def bar(self, key_word_sep = " ", title=None, **kwargs):
        """Generates a pylab bar plot from the result set.

        ``matplotlib`` must be installed, and in an
        IPython Notebook, inlining must be on::

            %%matplotlib inline

        The last quantitative column is taken as the Y values;
        all other columns are combined to label the X axis.

        Parameters
        ----------
        title: Plot title, defaults to names of Y value columns
        key_word_sep: string used to separate column values
                      from each other in labels

        Any additional keyword arguments will be passsed
        through to ``matplotlib.pylab.bar``.
        """
        import matplotlib.pylab as plt
        self.guess_pie_columns(xlabel_sep=key_word_sep)
        plot = plt.bar(range(len(self.ys[0])), self.ys[0], **kwargs)
        if self.xlabels:
            plt.xticks(range(len(self.xlabels)), self.xlabels,
                       rotation=45)
        plt.xlabel(self.xlabel)
        plt.ylabel(self.ys[0].name)
        return plot

    def csv(self, filename=None, **format_params):
        """Generate results in comma-separated form.  Write to ``filename`` if given.
           Any other parameters will be passed on to csv.writer."""
        if not self.pretty:
            return None # no results
        self.pretty.add_rows(self)
        if filename:
            encoding = format_params.get('encoding', 'utf-8')
            if six.PY2:
                outfile = open(filename, 'wb')
            else:
                outfile = open(filename, 'w', newline='', encoding=encoding)
        else:
            outfile = six.StringIO()
        writer = UnicodeWriter(outfile, **format_params)
        writer.writerow(self.field_names)
        for row in self:
            writer.writerow(row)
        if filename:
            outfile.close()
            return CsvResultDescriptor(filename)
        else:
            return outfile.getvalue()

    def render_pie(self, key_word_sep=" ", title=None, **kwargs):
        """Generates a pylab pie chart from the result set.

        ``matplotlib`` must be installed, and in an
        IPython Notebook, inlining must be on::

            %%matplotlib inline

        First column is color-axis, second column is numeric

        Parameters
        ----------
        key_word_sep: string used to separate column values
                      from each other in pie labels
        title: Plot title, defaults to name of value column

        Any additional keyword arguments will be passsed
        through to ``matplotlib.pylab.pie``.
        """
        self.build_columns()
        import matplotlib.pylab as plt
        pie = plt.pie(self.columns[1], labels=self.columns[0], **kwargs)
        plt.title(title or self.columns[1].name)
        if self.show_chart:
            plt.show()
        return pie


    def render_barh(self, key_word_sep=" ", title=None, **kwargs):
        """Generates a pylab horizaontal barchart from the result set.

        ``matplotlib`` must be installed, and in an
        IPython Notebook, inlining must be on::

            %%matplotlib inline

        First column is x-axis, and can be text, datetime or numeric. 
        Other columns are numeric, displayed as horizontal strips.

        Parameters
        ----------
        key_word_sep: string used to separate column values
                      from each other in pie labels
        title: Plot title, defaults to name of value column

        Any additional keyword arguments will be passsed
        through to ``matplotlib.pylab.pie``.
        """
        import matplotlib.pylab as plt
        self.build_columns()
        quantity_columns = [c for c in self.columns[1:] if c.is_quantity]
        ylabel = ", ".join([c.name for c in quantity_columns])
        xlabel = self.columns[0].name

        dim = len(quantity_columns)
        w = 0.8
        dimw = w / dim

        ax = plt.subplot(111)
        x = plt.arange(len(self.columns[0]))
        xpos = -dimw * (len(quantity_columns) / 2)
        for y in quantity_columns:
            barchart = plt.barh(x + xpos, y, align='center', **kwargs)
            # ax.barh(x + xpos, y, width = dimw, color='b', align='center', **kwargs)
            # ax.bar(y, width = dimw, height = w, x + xpos, *, align='center', **kwargs)
            xpos += dimw
        plt.yticks(range(len(self.columns[0])), self.columns[0], rotation=0)
        plt.ylabel(xlabel)
        plt.xlabel(ylabel)
        plt.title(title or ylabel)

        if self.show_chart:
            plt.show()
        return barchart

    def render_bar(self, key_word_sep=" ", title=None, **kwargs):
        """Generates a pylab horizaontal barchart from the result set.

        ``matplotlib`` must be installed, and in an
        IPython Notebook, inlining must be on::

            %%matplotlib inline

        First column is x-axis, and can be text, datetime or numeric. 
        Other columns are numeric, displayed as horizontal strips.

        Parameters
        ----------
        key_word_sep: string used to separate column values
                      from each other in pie labels
        title: Plot title, defaults to name of value column

        Any additional keyword arguments will be passsed
        through to ``matplotlib.pylab.pie``.
        """
        import matplotlib.pylab as plt
        self.build_columns()
        quantity_columns = [c for c in self.columns[1:] if c.is_quantity]
        ylabel = ", ".join([c.name for c in quantity_columns])
        xlabel = self.columns[0].name

        # print("xlabel: {}".format(xlabel))
        # print("ylabel: {}".format(ylabel))

        dim = len(quantity_columns)
        w = 0.8
        dimw = w / dim

        ax = plt.subplot(111)
        x = plt.arange(len(self.columns[0]))
        xpos = -dimw * (len(quantity_columns) / 2)
        for y in quantity_columns:
            columnchart = plt.bar(x + xpos, y, width = dimw, align='center', **kwargs)
            xpos += dimw
        plt.xticks(range(len(self.columns[0])), self.columns[0], rotation = 45)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.title(title or ylabel)

        if self.show_chart:
            plt.show()
        return columnchart


    def render_linechart(self, key_word_sep=" ", title=None, **kwargs):
        """Generates a pylab plot from the result set.

        ``matplotlib`` must be installed, and in an
        IPython Notebook, inlining must be on::

            %%matplotlib inline

        First column is x-axis, and should be a numeric column. Other numeric columns are y-axes.

        Parameters
        ----------
        title: Plot title, defaults to names of Y value columns

        Any additional keyword arguments will be passsed
        through to ``matplotlib.pylab.plot``.
        """

        import matplotlib.pyplot as plt
        self.build_columns()
        quantity_columns = [c for c in self.columns if c.is_quantity]
        if len(quantity_columns) < 2:
            return None
        x = quantity_columns[0]
        ys = quantity_columns[1:]
        ylabel = ", ".join([c.name for c in ys])
        xlabel = x.name

        coords = functools.reduce(operator.add, [(x, y) for y in ys])
        plot = plt.plot(*coords, **kwargs)
        plt.title(title or ylabel)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        if self.show_chart:
            plt.show()
        return plot


    def render_areachart(self, key_word_sep=" ", title=None, **kwargs):
        """Generates a pylab plot from the result set.

        ``matplotlib`` must be installed, and in an
        IPython Notebook, inlining must be on::

            %%matplotlib inline

        First column is x-axis, and should be a numeric column. Other numeric columns are y-axes.
        kind = default, unstacked, stacked, stacked100 

        Parameters
        ----------
        title: Plot title, defaults to names of Y value columns

        Any additional keyword arguments will be passsed
        through to ``matplotlib.pylab.plot``.
        """

        import matplotlib.pyplot as plt
        self.build_columns()
        quantity_columns = [c for c in self.columns if c.is_quantity]
        if len(quantity_columns) < 2:
            return None
        x = quantity_columns[0]
        ys = quantity_columns[1:]
        ylabel = ", ".join([c.name for c in ys])
        xlabel = x.name

        coords = functools.reduce(operator.add, [(x, y) for y in ys])
        plot = plt.plot(*coords, **kwargs)
        plt.xticks(range(len(x)), x, rotation = 45)
        plt.title(title or ylabel)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        if self.show_chart:
            plt.show()
        return plot



    def render_anomalychart(self, key_word_sep=" ", title=None, **kwargs):
        import plotly
        plotly.offline.init_notebook_mode(connected=True)

        import numpy as np
        import matplotlib.pyplot as plt
        import plotly.plotly as py
        import plotly.graph_objs as go


        # create our stacked data manually
        y0 = np.random.rand(100)
        y1 = y0 + np.random.rand(100)
        y2 = y1 + np.random.rand(100)
        capacity = 3*np.ones(100)

        x0 = list(range(len(y0)))

        trace0 = go.Scatter(
            x=x0,
            y=y0,
            mode='lines',
            line=dict(width=0.5,
                      color='rgb(184, 247, 212)'),
            fill='tonexty'
        )
        trace1 = go.Scatter(
            x=x0,
            y=y1,
            mode='lines',
            line=dict(width=0.5,
                      color='rgb(111, 231, 219)'),
            fill='tonexty'
        )
        trace2 = go.Scatter(
            x=x0,
            y=y2,
            mode='lines',
            line=dict(width=0.5,
                      color='rgb(127, 166, 238)'),
            fill='tonexty'
        )

        traceC = go.Scatter(
            x=x0,
            y=capacity,
            mode='lines',
            line=dict(width=0.5,
                      color='rgb(131, 90, 241)'),
            fill='tonexty'
        )
        data = [trace0, trace1, trace2, traceC]
        layout = go.Layout(
            showlegend=True,
            xaxis=dict(
                type='category',
            ),
            yaxis=dict(
                type='linear',
                range=[0, 3],
                dtick=20,
                ticksuffix='%'
            )
        )
        fig = go.Figure(data=data, layout=layout)
        plotly.offline.iplot(fig, filename='stacked-area-plot')
        return fig


class PrettyTable(prettytable.PrettyTable):

    # Object constructor
    def __init__(self, *args, **kwargs):
        self.row_count = 0
        self.displaylimit = None
        return super(PrettyTable, self).__init__(*args,  **kwargs)

    def add_rows(self, data):
        if self.row_count and (data.config.displaylimit == self.displaylimit):
            return  # correct number of rows already present
        self.clear_rows()
        self.displaylimit = data.config.displaylimit
        if self.displaylimit == 0:
            self.displaylimit = None  # TODO: remove this to make 0 really 0
        if self.displaylimit in (None, 0):
            self.row_count = len(data)
        else:
            self.row_count = min(len(data), self.displaylimit)
        for row in data[:self.displaylimit]:
            self.add_row(row)


