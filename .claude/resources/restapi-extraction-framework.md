> This document is an 8-step framework for extracting data from a REST API

## Step 1. Gather/ interrogate requirements

So, someone in your business has expressed a desire to get data from a particular source system, and that source system has a REST API? The first step is to play the consultant and do your best to ask the right questions:

Some questions that you should ask/ answer at this stage:

- Can this data be extracted in any other ways? Connector? Python library?
- What data does the business need to satisfy user stories/ analysis use cases?
  - "All of it" - might be their response, but the development process will take time, and the more tables you are asked to extract, the longer the development time. Make sure they are aware of this.
  - At the same time, you don't want to be too lean, only grabbing four fields from the API, and then 3 months later, it turns out they wanted additional columns - this means rework - new tables defined etc.

Ideally at the end of this stage, you should have:

- **buy-in**: you should have engaged the right stakeholders, they feel they are a part of the process.
- **requirements**: have a list of tables, and even key columns that are required for each table.

## Step 2: API documentation research phase

With some understanding of the requirements, now it's time to look at feasibility. You should find the REST API documentation for the source system.

Accessing REST API documentation is normally done through:

- Googling "{system name} REST API documentation", it should come up for most SaaS companies.
- Sometimes the documentation will not be public, you'll have to get access from a representative of the company, or via a private portal.
- If the REST API doesn't have official documentation - good luck! At the very minimum, I would expect some [Swagger documentation](https://swagger.io/tools/swagger-ui/ "https://swagger.io/tools/swagger-ui/") to understand what endpoints exists.

Once you have access to the documentation, you will need to answer the following questions:

1. How can I authenticate with this API? This is one of the most important questions to answer here. I recommend testing authentication (using step 3, postman), because this can be one of the biggest blockers on these types of projects.
2. Can I get the data I need (from the requirements)? Which endpoints can I retrieve this data from? How do I need to structure my requests to these endpoints (headers, params, body?).
3. Are there any other data that might help the business answer their user stories better? These might not be in the requirements, but it's always worth a conversation with them ("would this data be useful?"). Better to add it in now, rather than having to revisit and adding new data in 3 months time!
4. Are there any rate limits I need to be aware of?

**Go/ No-go decision:**

At this point, you should have gathered enough information to make a solid go/no-go decision: is it possible to answer the requirements with what the REST API provides, and it is possible to build it within a given timeframe (there's normally always deadlines!!).

Sometimes, after assessing the state of the API, you should make the decision not to build an API connection. Maybe key columns/ datasets are not currently accessible via the API, meaning the currently value vs development effort equation doesn't quite work out.

## Step 3: Testing authentication & building requests in Postman

You've made the 'Go' decision, your stakeholders are on build, now you need to start the development process, and for me, this always starts in Postman.

Constructing REST API requests, and authenticating with APIs can be a fiddly business, and Postman makes it a lot simpler (it's UI-based).

Therefore, I recommend using it to figure out the exact structure of your requests: how to authenticate, which headers do I need to send for this particular endpoint? Do I need URI params? For POST requests, how should my body be structured?

You _could_ figure all this stuff out in a Fabric Notebook, but it would take way longer.

> ⭐ Another key benefit of creating a collection of Postman requests is: in three months time, when your ETL process fails, and you don't know the reason. You can fire off the requests through Postman to check whether it's the API that is faulty/down, or it's something going wrong in your code/ ETL process. This can drastically help with debugging!

At the end of Step 3, you should:

**Authentication**:

- understand and implement the exact method for authenticating with the API
- as a by-product of this: you should have generated the keys, app tokens, client id/secrets etc that your API requires for authentication.

**For all other requests**:

- you should have built requests for each endpoint needed to satisfy your business requirements. Therefore: you know exactly which headers to send, params, how to structure your body etc.
- you should understand the response structure and have a pretty good understanding of how you are going to pull the required data from it (step 4, we will expand on this!).

## Step 4: Plan and document extraction flow/ logic

By this stage, you have constructed all the required requests in Postman, and have reviewed the response structure for each.

This is a good start, but now we need to convert this disparate collection of API requests into a coherent extraction strategy. I like to do this in [Excalidraw](https://excalidraw.com "https://excalidraw.com") (of course 😀).

Here's an example to give you a flavour:

![image.png](https://assets.skool.com/f/8b75bab39da9411e9c7aea07e99f6d7b/2167673899b84cd5b5968f0d852f45cd01c76120db314dc3ac970cb23da53b35 "image.png")

I'm trying to visually figure out the sequential flow of how I'm going to extract data from this API - and the steps required. This is informed by my knowledge of the API endpoints, and the response structure for each endpoint (from Step 3).

I think it's essential to do this _before_ you write a line of code.

Some APIs are very complex, and nested, so you might have to string together three or four requests to get the data you actually want. Like a project management tool, that requires you to do something like this:

- First, do this: GET /projects
- Then, for each project: GET /projects/phases
- Then for each phase: GET /projects/phases/tasks
- etc.

Personally I find a visual depiction of the flow & logic really helps me to understand how I am going extract data from this API, AND it is great documentation for your team to understand how the logic works.

## Step 5: Build

Once you understand the control flow logic, you can convert that visual pseudo-code actual code (if you choose to use Notebooks, or User Data Functions), or into a Copy Data activity in a Pipeline.

One of the benefits of using Postman is that they have a simple export request feature which can help you get very close to working code, very quickly. It will normally always require some refactoring, but normally not much!

> ℹ️ Note: the choice of tool is up to you. Personally, I would recommend learning how to use Python to send off these requests. It's a bit like learning to drive in a manual gear-box car. Once you understand how it works, you will be able to query any API, and handle any edge-cases, because of the flexibility of code/ Python. Whereas, if you limit yourself to only knowing Data Pipeline Copy Data activity (like learning to drive in an Automatic gearbox car 🚗), you will frequently meet technical hurdles.

Normally, I use the visual pseudocode I produce in Step 4 to frame out my code, creating comment blocks for each section. I'll sometimes create separate Python functions for each of these steps in the diagram also, but that depends on what the step is.

At the end of this stage, you should:

- have built your tables
- have successfully loaded data into your tables for the first time
- have implemented the visual pseudocode logic in actual Python code (or Pipeline activities)

## Step 6: Test/ reinforce

Everything up until this point should be done in a Development environment. Next, there needs to be a iterative process of testing and 'reinforcing' the process.

One of the benefits of using Python (over Data Pipelines), is that we can build a series of unit tests to make the process much more reliable (when we first release, and over time).

At this stage, you want to critically assess your code, and ask: where could this go wrong? Where are the weak points? And then build tests, and [defensive programming mechanisms](https://en.wikipedia.org/wiki/Defensive_programming "https://en.wikipedia.org/wiki/Defensive_programming") to proactively build defenses against such occurrences.

Typical things to consider include:

- understand [HTTP request status codes](https://learn.microsoft.com/en-us/troubleshoot/developer/webapps/iis/health-diagnostic-performance/http-status-code "https://learn.microsoft.com/en-us/troubleshoot/developer/webapps/iis/health-diagnostic-performance/http-status-code") (2XX, 4XX, 5XX), and how to handle each of them?
- what if our request returns status code: 200 (success), but no data?
- what if the structure of the response changes? (Can be tricky to handle, but at least we want to log and notify early).

## Step 7: Document

Always document what you've built! In the case of building connections to REST APIs, I would expect the following to be documented (as a minimum!):

- Important details of the REST API (basically everything you found out in Step 2: API documentation research phase), include links to the documentation, details of how you authenticate, details of any tokens you have created and where they are stored (Azure Key Vault most likely, for Fabric use cases).
- Details of the API requests you have made
- Details of the visual pseudocode/ general control flow logic for your API requests.
- Details about any new tables you've created - you might have existing documentation requirements for this.

When you're writing your documentation, imagine a Junior Engineer reading what you've written - you should give enough details for them to follow what you've done, and WHY. Document critical decisions you've made along the way. Example: _"In the end, we chose to use this endpoint, rather than this other one, because we can retrieve more records/ it's more efficient/ we can retrieve all the data we need from it in one request"_

## Step 8: Monitoring/ optimizing/ bug-fixing

That's the easy bit out the way 😀 now comes the fun part - managing and maintaining your extraction pipelines over the long-run.

This is a little outside the scope for this particular series, but we can look at it in more detail in the future!

There is a whole series of knowledge required:

- effectively monitoring these extraction pipelines for problems,
- debugging tools and processes when errors occur,
- recover system state in the event of something going wrong,
- modularizing your code base across different APIs, and creating a metadata-driven management system.
