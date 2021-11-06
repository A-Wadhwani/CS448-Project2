# CS448-Project2
Most of the modifications were kept to the `nestedblock` package inside of `multibuffer`. All testing is done inside the `JoinBenchmarking.java` file. This testing includes three components:

1.	Testing Correctness `correctnessTest()`: Addresses Part 3
2.	Testing Selection of Appropriate Join Algorithm testSelection(): Addresses Part 4
3.	Performance Comparison `runTests()`: Addresses Part 5.

Running any of these components is simply done by calling the corresponding function in `main()`.
Using the provided directions to run queries would still work but using JoinBenchmarking.java allows detailed tests for large sized queries.

To enable selection of join plans during testing for Performance Comparison, a debug mode flag was added to table planner, among other changes, to ensure that a specific plan could be selected and run. This flag is off by default though, so there should be no issue running the usual queries.
