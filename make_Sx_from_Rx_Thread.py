#

from math import sqrt, atan2, sin, cos, pi
from PyQt5.QtCore import QThread, pyqtSignal
import os


def calculate_offset_point(start, end, offset_distance, side):
    """
    Calculate a perpendicular offset point from the midpoint of a line segment.
    :param start: Tuple (x, y) for the start point of the line segment.
    :param end: Tuple (x, y) for the end point of the line segment.
    :param offset_distance: The distance for the offset.
    :param side: 'left' or 'right' for the offset direction.
    :return: The offset point as (x, y).
    """
    # Calculate the midpoint
    mid_x = (start[0] + end[0]) / 2
    mid_y = (start[1] + end[1]) / 2

    # Calculate the angle of the line
    dx = end[0] - start[0]
    dy = end[1] - start[1]
    angle = atan2(dy, dx)

    # Determine perpendicular angle based on the side
    if side == 'left':
        perpendicular_angle = angle + (pi / 2)
    elif side == 'right':
        perpendicular_angle = angle - (pi / 2)
    else:
        raise ValueError("Invalid side. Must be 'left' or 'right'.")

    # Calculate offset point
    offset_x = mid_x + offset_distance * cos(perpendicular_angle)
    offset_y = mid_y + offset_distance * sin(perpendicular_angle)

    return offset_x, offset_y


class make_Sx_from_Rx_Thread(QThread):
    make_Sx_from_Rx_finished = pyqtSignal()

    def __init__(self, myvar, parent=None):
        QThread.__init__(self, parent)
        self.reciver_working_array = myvar[0]
        self.side = myvar[1]
        self.offset = myvar[2]

    def run(self):
        try:
            print('make_Sx_from_Rx')
            # Ensure the temp directory exists
            os.makedirs('temp', exist_ok=True)
            outfile = open('temp/temp.csv', 'w')

            for i in range(1, len(self.reciver_working_array)):
                pt1 = self.reciver_working_array[i - 1]
                pt2 = self.reciver_working_array[i]
                line = str(pt1[0])
                new_line_check = str(pt2[0])

                if line == new_line_check:  # Skip if the line changes
                    stn_1 = int(pt1[1])
                    stn_2 = int(pt2[1])

                    shot_point = str(stn_1 + 0.5)

                    elev_1 = float(pt1[4])
                    elev_2 = float(pt2[4])
                    shot_point_elev = str(round((elev_1 + elev_2) / 2.0, 2))

                    Easting_1 = float(pt1[2])
                    Northing_1 = float(pt1[3])
                    Easting_2 = float(pt2[2])
                    Northing_2 = float(pt2[3])

                    a = (Easting_1, Northing_1)
                    b = (Easting_2, Northing_2)

                    # Calculate left and right offsets manually
                    left_offset = calculate_offset_point(a, b, self.offset, 'left')
                    right_offset = calculate_offset_point(a, b, self.offset, 'right')

                    c = left_offset  # (x, y) of left offset
                    d = right_offset  # (x, y) of right offset

                    # Write results based on the selected side
                    if self.side == 'Left':
                        new_line = f"{line},{shot_point},{round(c[0], 2)},{round(c[1], 2)},{shot_point_elev}"
                        outfile.write(new_line.rstrip('\n') + '\n')

                    if self.side == 'Right':
                        new_line = f"{line},{shot_point},{round(d[0], 2)},{round(d[1], 2)},{shot_point_elev}"
                        outfile.write(new_line.rstrip('\n') + '\n')

                    if self.side == 'Both':
                        new_line = f"{line},{shot_point},{round(c[0], 2)},{round(c[1], 2)},{shot_point_elev}"
                        outfile.write(new_line.rstrip('\n') + '\n')
                        new_line = f"{line}0,{shot_point},{round(d[0], 2)},{round(d[1], 2)},{shot_point_elev}"
                        outfile.write(new_line.rstrip('\n') + '\n')

            outfile.close()
            self.make_Sx_from_Rx_finished.emit()

        except Exception as e:
            print(f"Error in make_Sx_from_Rx: {e}")

# from shapely.geometry import LineString
# from PyQt5.QtCore import QObject, QThread, pyqtSignal
#
#
# class make_Sx_from_Rx_Thread(QThread):
#     make_Sx_from_Rx_finished = pyqtSignal()
#     def __init__(self, myvar, parent=None):
#         QThread.__init__(self, parent)
#         self.reciver_working_array = myvar[0]
#         self.side = myvar[1]
#         self.offset = myvar[2]
#     def run(self):
#         try:
#             print('make_Sx_from_Rx')
#             outfile = open('temp\\temp.csv', 'w')
#
#             for i in range(1, len(self.reciver_working_array)):
#                 pt1 = self.reciver_working_array[i-1]
#                 pt2 = self.reciver_working_array[i]
#                 line = str(pt1[0])
#                 new_line_check = str(pt2[0])
#
#                 if line == new_line_check:    ##  if line changes  skip
#
#                     stn_1 = int(pt1[1])
#                     stn_2 = int(pt2[1])
#
#                     shot_point = str(stn_1 + 0.5)
#
#                     elev_1 = float(pt1[4])
#                     elev_2 = float(pt2[4])
#
#                     shot_point_elev = str(round((elev_1 + elev_2) / 2.0, 2))
#
#                     Easting_1 = float(pt1[2])
#                     Northing_1 = float(pt1[3])
#
#                     Easting_2 = float(pt2[2])
#                     Northing_2 = float(pt2[3])
#
#                     midpoint = ((Easting_1 + Easting_2) / 2, (Northing_1 + Northing_2) / 2)
#
#                     a = (Easting_1, Northing_1)
#                     b = (midpoint[0], midpoint[1])
#                     print('31')
#                     ab = LineString([a, b])
#                     left = ab.parallel_offset(self.offset, 'left')
#                     right = ab.parallel_offset(self.offset, 'right')
#
#                  #   if len(left.boundary.geoms) >= 2 and len(right.boundary.geoms) >= 1:
#                     c = left.boundary.geoms[1]  # End point of the left offset
#                     d = right.boundary.geoms[0]  # Start point of the right offset
#                   #  else:
#                   #      raise ValueError("Offset boundaries are not as expected.")
#
#
#
#                     # print('32',left,right)
#                     # c = left.boundary[1]
#                     # print('32b')
#                     # d = right.boundary[0]  # note the different orientation for right offset
#
#                     print('33')
#                     if self.side == 'Left':
#                         new_line = line + ',' + shot_point + ',' + str(round(c.x, 2)) + ',' + str(
#                             round(c.y, 2)) + ',' + shot_point_elev
#                         outfile.write(new_line.rstrip('\n') + '\n')
#
#                     if self.side == 'Right':
#                         new_line = line + ',' + shot_point + ',' + str(round(d.x, 2)) + ',' + str(
#                             round(d.y, 2)) + ',' + shot_point_elev
#                         outfile.write(new_line.rstrip('\n')+'\n')
#
#                     if self.side == 'Both':
#                         new_line = line + ',' + shot_point + ',' + str(round(c.x, 2)) + ',' + str(
#                             round(c.y, 2)) + ',' + shot_point_elev
#                         outfile.write(new_line.rstrip('\n') + '\n')
#                         new_line = line+'0' + ',' + shot_point + ',' + str(round(d.x, 2)) + ',' + str(
#                             round(d.y, 2)) + ',' + shot_point_elev
#                         outfile.write(new_line.rstrip('\n')+'\n')
#
#
#             outfile.close()
#             self.make_Sx_from_Rx_finished.emit()
#
#
#         except:
#             print('error make_Sx_from_Rx')
#
#
