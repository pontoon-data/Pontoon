# AWS Fargate Deployment

Deploy Pontoon on AWS Fargate for scalable, managed container orchestration with auto-scaling capabilities.

## Quick Start

### What you'll need...
- An AWS Account
- AWS credentials or IAM Role with permission to create VPCs, Subnets, Security Groups, ECS clusters, ECS Task Definitions and run ECS Tasks
- The AWS default `ecsTaskExecution` IAM role created in your account
- An AWS CloudWatch Log Group named `/ecs/pontoon-fargate` 
- Optional, but highly recommended: your own Redis 7+ and PostgreSQL 16+ instances

> 💡 This guide is intended to provide a very simple example of deloyment on AWS ECS Fargate. We strongly recommend that you adapt this guide to your environment and follow security and availability best practices as needed.


### External Redis and PostgreSQL

We highly recommend using your own external Redis and PostgreSQL instances for security, reliability, durability and performance. 

To configure your own external data stores:

- Update the `POSTGRES_*` and `CELERY_*` variables in `fargate.env` and ensure those endpoints are accessible from Subnets used by your ECS Task
- Remove the `redis` and `postgres` services from `docker-compose.yml` and `docker-compose.fargate.yml`

### Step 1: Create an ECS cluster

- Install the [Amazon ECS CLI](https://github.com/aws/amazon-ecs-cli)
- Follow the instructions to configure your AWS credentials
- If you're using an existing AWS credentials profile, include the `--aws-profile <name>` on `ecs-cli` commands that follow   

You can use an existing cluster, or create a new one:
```bash
$ ecs-cli up --cluster pontoon --launch-type FARGATE
INFO[0000] Created cluster cluster=pontoon region=us-west-2
INFO[0001] Waiting for your cluster resources to be created... 
INFO[0001] Cloudformation stack status stackStatus=CREATE_IN_PROGRESS
VPC created: vpc-0ee3ff5453578221b
Subnet created: subnet-0441b3260e16368bd
Subnet created: subnet-04b7f89ee386776b6
Cluster creation succeeded.

```

### Step 2: Update ecs-params.yml

Add your new or existing Subnet IDs to the ECS configuration file:
```yaml
...

run_params:
  network_configuration:
    awsvpc_configuration:
      subnets:
        - subnet-0441b3260e16368bd
        - subnet-04b7f89ee386776b6
      assign_public_ip: ENABLED

...
```

### Step 3: Start Pontoon 

Start Pontoon using ECS CLI and the Docker Compose definitions:
```bash
$ ecs-cli compose -f docker-compose.yml -f docker-compose.fargate.yml up --launch-type FARGATE
INFO[0000] Using ECS task definition                     TaskDefinition="pontoon:1"
INFO[0000] Auto-enabling ECS Managed Tags               
INFO[0001] Starting container...                         container=pontoon-fargate/4bbead2ac77a47c0b573d2481045d15d/api
INFO[0001] Starting container...                         container=pontoon-fargate/4bbead2ac77a47c0b573d2481045d15d/beat
INFO[0001] Starting container...                         container=pontoon-fargate/4bbead2ac77a47c0b573d2481045d15d/frontend
INFO[0001] Starting container...                         container=pontoon-fargate/4bbead2ac77a47c0b573d2481045d15d/postgres
INFO[0001] Starting container...                         container=pontoon-fargate/4bbead2ac77a47c0b573d2481045d15d/redis
INFO[0001] Starting container...                         container=pontoon-fargate/4bbead2ac77a47c0b573d2481045d15d/worker
INFO[0001] Describe ECS container status                 container=pontoon-fargate/4bbead2ac77a47c0b573d2481045d15d/redis desiredStatus=RUNNING lastStatus=PROVISIONING taskDefinition="pontoon:1"
```

Check that your task containers are up and running:

```bash
$ ecs-cli compose ps
Name    State    Ports    TaskDefinition  Health
pontoon-fargate/4bbead2ac77a47c0b573d2481045d15d/redis     RUNNING  15.223.209.32:6379->6379/tcp  pontoon:1       HEALTHY
pontoon-fargate/4bbead2ac77a47c0b573d2481045d15d/frontend  RUNNING  15.223.209.32:3000->3000/tcp  pontoon:1       UNKNOWN
pontoon-fargate/4bbead2ac77a47c0b573d2481045d15d/api       RUNNING  15.223.209.32:8000->8000/tcp  pontoon:1      UNKNOWN
pontoon-fargate/4bbead2ac77a47c0b573d2481045d15d/worker    RUNNING                                pontoon:1       UNKNOWN
pontoon-fargate/4bbead2ac77a47c0b573d2481045d15d/postgres  RUNNING  15.223.209.32:5432->5432/tcp  pontoon:1       HEALTHY
pontoon-fargate/4bbead2ac77a47c0b573d2481045d15d/beat      RUNNING                                pontoon:1       UNKNOWN
```

### Step 4: Run Database Migrations

Apply the database migrations to your `postgres` instance -- connection details will depend on how you've chosen to run PostgreSQL.

```bash
$ psql -h 15.224.219.33 -U dev -d pontoon -f api/db/migrations/V0001__initial_pontoon_schema.sql  
```

### Step 5: Access the Web UI

> 💡 You may need to modify the Security Group for your ECS Task to allow external access to port 3000 

Navigate to the public IP or DNS name for your ECS task using port `:3000`:

[http://15.224.219.33:3000/](http://15.224.219.33:3000/)

Done! 🚀

