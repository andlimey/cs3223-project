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
The Block Nested Loops Join (BNJ) implementation is modified from the given Nested Loops Join implementation.  
The block size for BNJ is calculated using number of buffers - 2.  
In the `open()` method, a new cursor is initailised for the outer block and a new boolean is initialised for the end of block object.  
In the `next()` method, we simulate a block using Java's ArrayList. For each block, we will iterate through each left page and right page, matching the left tuples to the right tuples.  
Once the outbatch is full, we will keep track of the cursor of the block, left and right for the next iteration.  
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

### Bug(s) Fixed
Bug 1: Plancost of Nested Join is incorrect
- The joinCost was calculated as `leftpages * rightpages` when it should be `leftpages + leftpages * rightpages`
