# Amazon Kinesis Data Visualization Sample Application

The **Amazon Kinesis Data Visualization Sample Application** demonstrates how to interact with [Amazon Kinesis][kinesis] to generate meaningful statistics from a stream of data and visualize those results in real time. 

* [Kinesis Product Page][kinesis]
* [Forum][kinesis-forum]
* [Issues][issues]

## Features

The Amazon Kinesis Data Visualization Sample Application contains three components:

1. A record publisher to send data to [Amazon Kinesis][kinesis].
1. An [Amazon Kinesis Client][kcl] application to compute the number of HTTP requests a resource received, and the HTTP referrer that sent them, over a sliding window.
1. An embedded web server and real time chart to display counts as they are computed.

The application will create one Amazon Kinesis stream with two shards and two Amazon DynamoDB tables in your AWS account.

Important: These resources will incur charges on your AWS bill. It is your responsibility to delete these resources. A utility for deleting them is provided as part of this application. See [Deleting Sample Application Resources](#deleting-sample-application-resources) for more information.

This application also includes a [CloudFormation][cloudformation] template to demonstrate how to launch an Amazon Kinesis Client application on EC2.

## Getting Started

1. **Sign up for AWS** &mdash; Before you begin, you need an AWS account. For more information about creating an AWS account and retrieving your AWS credentials, see [AWS Account and Credentials][docs-signup] in the AWS SDK for Java Developer Guide.
1. **Sign up for Amazon Kinesis** &mdash; Go to the Amazon Kinesis console to sign up for the service.
1. **Minimum requirements** &mdash; To use the sample application, you'll need **Java 1.7+** and [Maven 3][maven]. For more information about the requirements, see the [Getting Started][kinesis-getting-started] section of the Amazon Kinesis Developer Guide.
1. **Using the Amazon Kinesis Data Visualization Sample Application** &mdash; The best way to familiarize yourself with the sample application is to read the [Getting Started][kinesis-getting-started] section of the Amazon Kinesis Developer Guide.

## Building from Source

You can build the sample application using Maven:

```
mvn clean package
```

The application and all its dependencies are packaged into ```target/amazon-kinesis-data-visualization-sample-1.0.0-assembly.zip```. The CloudFormation template relies on this archive and its structure to start the application on the EC2 host.

## Running the Application

You can execute all the sample application components directly from the command line using Maven.

### Stream Writer

[HttpReferrerStreamWriter.java](src/main/java/com/amazonaws/services/kinesis/samples/datavis/HttpReferrerStreamWriter.java) sends randomly generated records to the Amazon Kinesis stream. You can start it locally by executing:

```MAVEN_OPTS="-Daws.accessKeyId=YOURACCESSKEY -Daws.secretKey=YOURSECRETKEY" mvn compile -Pstream-writer exec:java```

### Counting Kinesis Client Application

[HttpReferrerCounterApplication.java](src/main/java/com/amazonaws/services/kinesis/samples/datavis/HttpReferrerCounterApplication.java) starts a Kinesis Client application that counts the number of HTTP requests to a resource over a sliding window. The application then persists the counts to DynamoDB for retrieval by the web application. You can start it locally by executing:

```MAVEN_OPTS="-Daws.accessKeyId=YOURACCESSKEY -Daws.secretKey=YOURSECRETKEY" mvn compile -Pcounter exec:java```

### Real Time Chart (Web Server)

[WebServer.java](src/main/java/com/amazonaws/services/kinesis/samples/datavis/WebServer.java) starts a web server on port 8080 to view the results of the Counting Kinesis Client Application in real time. You can start it locally by executing:

```MAVEN_OPTS="-Daws.accessKeyId=YOURACCESSKEY -Daws.secretKey=YOURSECRETKEY" mvn compile -Pwebserver exec:java```

After you have started all the components, navigate to http://localhost:8080 to view the running application. Note that you must execute both the Stream Writer and the Counting Kinesis Client Application to see any data on the chart.

### CloudFormation Template

A sample CloudFormation template is included to demonstrate how to launch the application on EC2. The template provisions an EC2 t1.micro instance and starts all three applications on it. See [EC2 Instance Types][ec2-instance-types] for more information on instance types.

The CloudFormation stack creates an [IAM Role][iam-role] to allow the application to authenticate your account without the need for you to provide explicit credentials. See [Using IAM Roles for EC2 Instances with the SDK for Java][iam-roles-java-sdk] for more information.

The template can be found at ```src/main/static-content/cloudformation/kinesis-data-vis-sample-app.template```.

Visit the [AWS CloudFormation][cloudformation] page for more information on what CloudFormation is and how you can leverage it to create and manage AWS resources.

## Deleting Sample Application Resources

The sample application creates one Amazon Kinesis Stream and two Amazon DynamoDB tables, which will bill to your account. You can delete these resources by executing the following command:

```MAVEN_OPTS="-Daws.accessKeyId=YOURACCESSKEY -Daws.secretKey=YOURSECRETKEY" mvn compile -Pdelete-resources exec:java```

[kinesis]: http://aws.amazon.com/kinesis
[kcl]: https://github.com/awslabs/amazon-kinesis-client
[kinesis-forum]: http://developer.amazonwebservices.com/connect/forum.jspa?forumID=169
[issues]: https://github.com/awslabs/amazon-kinesis-data-visualization-sample/issues
[docs-signup]: http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/java-dg-setup.html
[kinesis-guide]: http://docs.aws.amazon.com/kinesis/latest/dev/introduction.html
[kinesis-getting-started]: http://docs.aws.amazon.com/kinesis/latest/dev/getting-started.html
[kinesis-guide-begin]: http://docs.aws.amazon.com/kinesis/latest/dev/before-you-begin.html
[kinesis-guide-create]: http://docs.aws.amazon.com/kinesis/latest/dev/step-one-create-stream.html
[kinesis-guide-applications]: http://docs.aws.amazon.com/kinesis/latest/dev/kinesis-record-processor-app.html
[cloudformation]: http://aws.amazon.com/cloudformation
[ec2-instance-types]: http://aws.amazon.com/ec2/instance-types
[iam-role]: http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html
[iam-roles-java-sdk]: http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/java-dg-roles.html
[maven]: http://maven.apache.org/
