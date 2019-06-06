#Sanjay Roberts

import math

class Points:

    def __init__(self, name, X_val, Y_val, cell_location=None):
        self.name = name
        self.X_val = X_val
        self.Y_val = Y_val
        self.cell_location = cell_location if cell_location is not None else None

    def __repr__(self):
        return '({}, {}, {})'.format(self.name, self.X_val,self.Y_val)

    def __sortkey__(self):
        return self.name

    def __iter__(self):
       for x in [self.name, self.X_val, self.Y_val]:
           yield x

    def find_cells(self, cellSize):
        xCell = int(math.floor(self.X_val/cellSize))
        yCell = int(math.floor(self.Y_val/cellSize))
        otherCells = [(xCell, yCell), (xCell + cellSize, yCell), (xCell - cellSize, yCell + cellSize), (xCell ,yCell + cellSize), (xCell - cellSize, yCell)]
        l = []
        for cell in otherCells:
            l.append(Points_with_cell(cell, (self.name, self.X_val, self.Y_val)))
        return l

