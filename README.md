# Recruitment Project

## What to do
1. Create a new repository and push the original assignment to it
2. Create a pull request in your new repo with your solution, write a short description of your solution and flag for any constraints and/or trade-offs in your code. Treat it as you would like to treat any PR at work
3. Ask your contact person for reviewers to assign to your PR. Hopefully you'll exchange some comments & feedback before having a follow up discussion in person or online.

## Tasks:
There tasks test your ability to:
- Work with existing code. Most of our work is in editing existing code rather than writing stand alone packages. Being able to read other code to see what needs to be refactored is an important skill.
- Work with the tool we use. The code requests are simple, but with specific tools. Either you know these tools or are comfortable handling new tools from documentation.

Complete the following two tasks and push your code on a branch to the remote repository. The branch name should be your first and last names divided by a '-'. 

### Task 1:

Fill in the run_beam function in workflow.py with a beam pipeline replicating the steps done in run_multiprocess found in local.py

### Task 2:

Replace the body of linear_regression found in processing.py with a pytorch implementation of linear regression that:
1. Constrains the intercept to be between 0.0 and 1.0 inclusive
2. Constrains the slope to be greater than or equal to 0.0
3. Never returns NaNs

Try to do it in a way that is reasonably efficient.
