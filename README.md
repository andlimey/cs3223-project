# CS3223-Project

Include a README file that should cover sufficient information for the TA to figure out the implementation and understand the modifications easily.

List of JOINs Implemented
- Block Nested Loops Join
- Sort Merge Join

List of Operators Implemented
- Distinct
- Orderby
- Groupby

### Implementation of Block Nested Loops Join
### Implmentation of Sort Merge Join
### Implementation of Distinct
### Implementation of Orderby
The Orderby operator uses the external sorting algorithm to order the attributes. 

It sorts according to the list of attributes to order by and whether the tuples should be arranged in ascending or descending order.

### Implementation of Groupby
The Groupby operator leverages the external sorting algorithm used for `Orderby` and `Sort Merge Join` to partition the desired groups. 
The conditions required to be fulfilled are:
- Attribute in SELECT clause must appear in the Groupby Clause OR
- Attribute is a primary key.

If the above conditions are not fulfilled, RandomInitialPlan will not allow the query to pass.
