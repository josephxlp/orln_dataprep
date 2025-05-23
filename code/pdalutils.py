import pdal 

def pdal_quick_info(laz_path):
    reader = pdal.Reader(laz_path) # sensitive to resolution, can pass argument here 
    pipeline = reader.pipeline()
    qi = pipeline.quickinfo 
    return qi,pipeline

