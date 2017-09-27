# MapReduce-job-for-Spatial-Containment
Spatial  join  is  a  common  type  of  joins  in  many  applications  that  manage  multi-dimensional  data.  A typical example of spatial join is to have two datasets: Dataset  P(set of POINTS in two-dimensionalspacesuch as mobile devices on people or sensors on cars) as shown in Figure 1a, and Dataset R (set of RECTANGLES in two-dimensionalspacesuch as parking structures, buildings or regions in a city) as shown  in  Figure  1b.  The  spatial  join  operation  is  to  join  these  two  datasets  and  report  any  pair (rectangle r, point p) where pis contained inr(or even in the border of r).For instance, this would allow  us  to  determine  which  parking  lot R  agiven  car  P  is  in,  or  which  building  R  a  person  P  has entered.
## Step 1 (Create the Two Datasets)
In this task, you will create the two datasets P (set of 2D points) and R(set of 2D rectangles). 
- Each line in data fileP should denote an object be <x, y>where x and y are the coordinates (in integer). While for data file R, each line denotesa region as <ri, x-bottom-left, y-bottom-left, x-top-right,y-top-right>where ri denotes the identifier of the region and the rest denote the coordinates of the rectangle. 
- The space extends from [1 ... 10,000]in both the X and Y axis. 
- All data points in P must have coordinates X and Y that are integers as value(fall on a line). 
- Scale one of the datasetsPor Rto be at least 100MB.
- Choose the appropriate random function (of your choice) to create the points. For the rectangles, you couldfor example select a point at random (say the bottom-left corner), and then select tworandom variablesthat define the heightand widthof the rectangle.For example, you should set the  height  random  variable  uniformlybetween  [1,20]  and  the  width  uniformlybetween  [1,5]. Make sure that all rectangles are fully contained within the given space. 
## Step 2(MapReduce job for Spatial Containment Join)
In this task, you develop your big data processing solution.
- First, you will write a java map-reduce job that implements the spatial join operation between the two datasets P and R. 
- Then,  adjust  your  program  to  take  an  optional  input  parameter  W(x1,  y1,  x2,  y2)  that indicates  a  spatial  window  (rectangle) of  interest  within  which  we  want  to  report  the  joined objects. If W is omitted, then the entire two sets should be joined. For example, referring to Figure  1,  if  the  window  parameter  is  W(1,  3,  3,  20),  then  the  reported  joined  objects  should be:<r1, (3,15)> <r2, (2,4)><r3, (2,4)>
- If your above solution uses more than one map-reduce job, then rewrite your solution to now be realized as one single map-reduce job.Explain assumption you make, if any.
