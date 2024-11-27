

def calculate_bounding_boxes(extent, chunk_width = 100000, chunk_height = 100000):
    """ 
    Split an extent from a geofile into non overalpping sub boxes for seperate processing 
    """
    # add check for zero extent 
    if extent[0] == extent[1] or extent[2] == extent[3]:
        return [(extent[0], extent[2], extent[1], extent[3])]
    minX, maxX, minY, maxY = extent
    bounding_boxes = []
    current_minX = minX
    while current_minX < maxX:
        current_minY = minY
        while current_minY < maxY:
            current_maxX = min(current_minX + chunk_width, maxX)
            current_maxY = min(current_minY + chunk_height, maxY)
            bounding_boxes.append((current_minX, current_minY, current_maxX, current_maxY))
            current_minY += chunk_height
        current_minX += chunk_width
    return bounding_boxes

