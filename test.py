import boto3
import json
import time
from boto3.dynamodb.conditions import Attr

# ─────────────────────────────────────────
# CONFIG — Update these values
# ─────────────────────────────────────────
AWS_REGION = "us-east-1"
DYNAMODB_TABLE_NAME = "eni-registry"
ASG_NAME = "your-asg-name"

# 3 sets × 4 ENIs = 12 total
ENI_SETS = [
    # ──── Set 1 ────
    {
        "set_id": "set-1",
        "name": "private-internal-1",
        "subnet_id": "subnet-aaa111",
        "private_ip": "10.0.1.50",
        "sgs": ["sg-private-internal"],
        "device_index": 1,
    },
    {
        "set_id": "set-1",
        "name": "management-1",
        "subnet_id": "subnet-bbb222",
        "private_ip": "10.0.2.50",
        "sgs": ["sg-management"],
        "device_index": 2,
    },
    {
        "set_id": "set-1",
        "name": "sterling-1",
        "subnet_id": "subnet-ccc333",
        "private_ip": "10.0.3.50",
        "sgs": ["sg-sterling"],
        "device_index": 3,
    },
    {
        "set_id": "set-1",
        "name": "public-external-1",
        "subnet_id": "subnet-ddd444",
        "private_ip": "10.0.4.50",
        "sgs": ["sg-public-external"],
        "device_index": 4,
        "eip_allocation_id": "eipalloc-aaa111",
    },

    # ──── Set 2 ────
    {
        "set_id": "set-2",
        "name": "private-internal-2",
        "subnet_id": "subnet-aaa111",
        "private_ip": "10.0.1.51",
        "sgs": ["sg-private-internal"],
        "device_index": 1,
    },
    {
        "set_id": "set-2",
        "name": "management-2",
        "subnet_id": "subnet-bbb222",
        "private_ip": "10.0.2.51",
        "sgs": ["sg-management"],
        "device_index": 2,
    },
    {
        "set_id": "set-2",
        "name": "sterling-2",
        "subnet_id": "subnet-ccc333",
        "private_ip": "10.0.3.51",
        "sgs": ["sg-sterling"],
        "device_index": 3,
    },
    {
        "set_id": "set-2",
        "name": "public-external-2",
        "subnet_id": "subnet-ddd444",
        "private_ip": "10.0.4.51",
        "sgs": ["sg-public-external"],
        "device_index": 4,
        "eip_allocation_id": "eipalloc-bbb222",
    },

    # ──── Set 3 ────
    {
        "set_id": "set-3",
        "name": "private-internal-3",
        "subnet_id": "subnet-aaa111",
        "private_ip": "10.0.1.52",
        "sgs": ["sg-private-internal"],
        "device_index": 1,
    },
    {
        "set_id": "set-3",
        "name": "management-3",
        "subnet_id": "subnet-bbb222",
        "private_ip": "10.0.2.52",
        "sgs": ["sg-management"],
        "device_index": 2,
    },
    {
        "set_id": "set-3",
        "name": "sterling-3",
        "subnet_id": "subnet-ccc333",
        "private_ip": "10.0.3.52",
        "sgs": ["sg-sterling"],
        "device_index": 3,
    },
    {
        "set_id": "set-3",
        "name": "public-external-3",
        "subnet_id": "subnet-ddd444",
        "private_ip": "10.0.4.52",
        "sgs": ["sg-public-external"],
        "device_index": 4,
        "eip_allocation_id": "eipalloc-ccc333",
    },
]


# ─────────────────────────────────────────
# AWS CLIENTS
# ─────────────────────────────────────────
ec2         = boto3.client("ec2", region_name=AWS_REGION)
autoscaling = boto3.client("autoscaling", region_name=AWS_REGION)
dynamodb    = boto3.resource("dynamodb", region_name=AWS_REGION)
ddb_client  = boto3.client("dynamodb", region_name=AWS_REGION)


# ─────────────────────────────────────────
# STEP 1 — CREATE DYNAMODB TABLE
# ─────────────────────────────────────────
def create_dynamodb_table():
    print("\n── Creating DynamoDB table ──")

    existing = ddb_client.list_tables()["TableNames"]
    if DYNAMODB_TABLE_NAME in existing:
        print(f"Table '{DYNAMODB_TABLE_NAME}' already exists, skipping.")
        return dynamodb.Table(DYNAMODB_TABLE_NAME)

    table = dynamodb.create_table(
        TableName=DYNAMODB_TABLE_NAME,
        KeySchema=[
            {"AttributeName": "eni_name", "KeyType": "HASH"},   # partition key
        ],
        AttributeDefinitions=[
            {"AttributeName": "eni_name", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
        GlobalSecondaryIndexes=[
            {
                "IndexName": "set_id-index",
                "KeySchema": [
                    {"AttributeName": "set_id", "KeyType": "HASH"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
            {
                "IndexName": "status-index",
                "KeySchema": [
                    {"AttributeName": "status", "KeyType": "HASH"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
        ],
        # GSI attribute definitions must be declared here too
        # Override AttributeDefinitions to include GSI keys
    )

    # Wait until table is active
    print("Waiting for table to become active...")
    waiter = ddb_client.get_waiter("table_exists")
    waiter.wait(TableName=DYNAMODB_TABLE_NAME)
    print(f"Table '{DYNAMODB_TABLE_NAME}' created successfully.")
    return table


def create_dynamodb_table():
    print("\n── Creating DynamoDB table ──")

    existing = ddb_client.list_tables()["TableNames"]
    if DYNAMODB_TABLE_NAME in existing:
        print(f"Table '{DYNAMODB_TABLE_NAME}' already exists, skipping.")
        return dynamodb.Table(DYNAMODB_TABLE_NAME)

    table = dynamodb.create_table(
        TableName=DYNAMODB_TABLE_NAME,
        KeySchema=[
            {"AttributeName": "eni_name", "KeyType": "HASH"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "eni_name",  "AttributeType": "S"},
            {"AttributeName": "set_id",    "AttributeType": "S"},
            {"AttributeName": "status",    "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
        GlobalSecondaryIndexes=[
            {
                "IndexName": "set_id-index",
                "KeySchema": [{"AttributeName": "set_id", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            },
            {
                "IndexName": "status-index",
                "KeySchema": [{"AttributeName": "status", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            },
        ],
    )

    print("Waiting for table to become active...")
    waiter = ddb_client.get_waiter("table_exists")
    waiter.wait(TableName=DYNAMODB_TABLE_NAME)
    print(f"Table '{DYNAMODB_TABLE_NAME}' created successfully.")
    return table


# ─────────────────────────────────────────
# STEP 2 — CREATE ENIs AND STORE IN DYNAMODB
# ─────────────────────────────────────────
def create_enis(table):
    print("\n── Creating ENIs ──")

    for eni_def in ENI_SETS:
        # Check if already exists in DynamoDB
        existing = table.get_item(Key={"eni_name": eni_def["name"]}).get("Item")
        if existing:
            print(f"  {eni_def['name']} already exists ({existing['eni_id']}), skipping.")
            continue

        # Create ENI
        response = ec2.create_network_interface(
            SubnetId=eni_def["subnet_id"],
            PrivateIpAddress=eni_def["private_ip"],
            Groups=eni_def["sgs"],
            Description=f"{eni_def['name']} - static ENI",
        )
        eni_id = response["NetworkInterface"]["NetworkInterfaceId"]

        # Tag the ENI
        ec2.create_tags(
            Resources=[eni_id],
            Tags=[
                {"Key": "Name",    "Value": eni_def["name"]},
                {"Key": "set_id",  "Value": eni_def["set_id"]},
                {"Key": "managed", "Value": "eni-lifecycle-manager"},
            ],
        )

        # Associate EIP if public
        if "eip_allocation_id" in eni_def:
            ec2.associate_address(
                AllocationId=eni_def["eip_allocation_id"],
                NetworkInterfaceId=eni_id,
            )
            print(f"  EIP {eni_def['eip_allocation_id']} associated with {eni_id}")

        # Store in DynamoDB
        table.put_item(Item={
            "eni_name":           eni_def["name"],
            "set_id":             eni_def["set_id"],
            "eni_id":             eni_id,
            "private_ip":         eni_def["private_ip"],
            "subnet_id":          eni_def["subnet_id"],
            "sgs":                eni_def["sgs"],
            "device_index":       eni_def["device_index"],
            "eip_allocation_id":  eni_def.get("eip_allocation_id", None),
            "status":             "available",
            "instance_id":        None,
        })

        print(f"  Created {eni_def['name']} → {eni_id} (IP: {eni_def['private_ip']})")

    print("All ENIs created.")


# ─────────────────────────────────────────
# STEP 3 — SETUP LIFECYCLE HOOKS
# ─────────────────────────────────────────
def setup_lifecycle_hooks():
    print("\n── Setting up ASG Lifecycle Hooks ──")

    # Launch hook
    autoscaling.put_lifecycle_hook(
        LifecycleHookName="attach-enis",
        AutoScalingGroupName=ASG_NAME,
        LifecycleTransition="autoscaling:EC2_INSTANCE_LAUNCHING",
        HeartbeatTimeout=300,
        DefaultResult="ABANDON",
    )
    print("  Launch hook created.")

    # Terminate hook
    autoscaling.put_lifecycle_hook(
        LifecycleHookName="detach-enis",
        AutoScalingGroupName=ASG_NAME,
        LifecycleTransition="autoscaling:EC2_INSTANCE_TERMINATING",
        HeartbeatTimeout=300,
        DefaultResult="CONTINUE",
    )
    print("  Terminate hook created.")


# ─────────────────────────────────────────
# STEP 4 — LAMBDA HANDLER (attach/detach)
# ─────────────────────────────────────────
def get_available_set(table):
    """Find one complete set of 4 available ENIs"""
    response = table.scan(
        FilterExpression=Attr("status").eq("available")
    )
    available_enis = response["Items"]

    # Group by set_id
    sets = {}
    for eni in available_enis:
        sid = eni["set_id"]
        if sid not in sets:
            sets[sid] = []
        sets[sid].append(eni)

    # Return first complete set
    for sid, enis in sets.items():
        if len(enis) == 4:
            print(f"  Found available set: {sid}")
            return enis

    return None


def complete_lifecycle_hook(data, result):
    autoscaling.complete_lifecycle_action(
        LifecycleHookName=data["LifecycleHookName"],
        AutoScalingGroupName=data["AutoScalingGroupName"],
        LifecycleActionToken=data["LifecycleActionToken"],
        LifecycleActionResult=result,
        InstanceId=data["EC2InstanceId"],
    )
    print(f"  Lifecycle hook completed with: {result}")


def handle_launch(data, table):
    instanceId = data["EC2InstanceId"]
    print(f"\n── Handling LAUNCH for {instanceId} ──")

    # Validate instance
    resp = ec2.describe_instances(InstanceIds=[instanceId])
    if not resp["Reservations"]:
        print("Instance not found")
        complete_lifecycle_hook(data, "ABANDON")
        return

    instance = resp["Reservations"][0]["Instances"][0]
    state = instance["State"]["Name"]
    if state not in ["pending", "running"]:
        print(f"Instance going down: {state}")
        complete_lifecycle_hook(data, "ABANDON")
        return

    # Get available ENI set
    available_set = get_available_set(table)
    if not available_set:
        print("No available ENI sets!")
        complete_lifecycle_hook(data, "ABANDON")
        return

    set_id = available_set[0]["set_id"]
    print(f"  Assigning {set_id} to {instanceId}")

    # Attach all 4 ENIs
    for eni in sorted(available_set, key=lambda x: x["device_index"]):
        print(f"  Attaching {eni['eni_name']} ({eni['eni_id']}) → device {eni['device_index']}")

        ec2.attach_network_interface(
            NetworkInterfaceId=eni["eni_id"],
            InstanceId=instanceId,
            DeviceIndex=int(eni["device_index"]),
        )

        # Mark as in-use
        table.update_item(
            Key={"eni_name": eni["eni_name"]},
            UpdateExpression="SET #s = :inuse, instance_id = :iid",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":inuse": "in-use",
                ":iid": instanceId,
            }
        )

    complete_lifecycle_hook(data, "CONTINUE")
    print(f"  All ENIs from {set_id} attached to {instanceId}")


def handle_terminate(data, table):
    instanceId = data["EC2InstanceId"]
    print(f"\n── Handling TERMINATE for {instanceId} ──")

    # Find ENIs for this instance
    response = table.scan(
        FilterExpression=Attr("instance_id").eq(instanceId)
    )
    enis = response["Items"]

    if not enis:
        print(f"No ENIs found for {instanceId}")
        complete_lifecycle_hook(data, "CONTINUE")
        return

    # Detach all ENIs
    for eni in enis:
        print(f"  Detaching {eni['eni_name']} ({eni['eni_id']})")
        try:
            eni_details = ec2.describe_network_interfaces(
                NetworkInterfaceIds=[eni["eni_id"]]
            )
            attachment = eni_details["NetworkInterfaces"][0].get("Attachment")
            if attachment:
                ec2.detach_network_interface(
                    AttachmentId=attachment["AttachmentId"],
                    Force=True,
                )
                print(f"  Detached {eni['eni_id']}")

        except Exception as e:
            print(f"  Error detaching {eni['eni_id']}: {e}")

        # Mark as available
        table.update_item(
            Key={"eni_name": eni["eni_name"]},
            UpdateExpression="SET #s = :available, instance_id = :none",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":available": "available",
                ":none": None,
            }
        )

    complete_lifecycle_hook(data, "CONTINUE")
    print(f"  All ENIs released from {instanceId}")


def lambda_handler(event, context):
    """Entry point for AWS Lambda"""
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)

    for record in event["Records"]:
        data = json.loads(record["body"])["detail"]
        transition = data["LifecycleTransition"]

        if transition == "autoscaling:EC2_INSTANCE_LAUNCHING":
            handle_launch(data, table)

        elif transition == "autoscaling:EC2_INSTANCE_TERMINATING":
            handle_terminate(data, table)

        else:
            print(f"Unknown transition: {transition}")


# ─────────────────────────────────────────
# STEP 5 — VERIFY / PRINT TABLE CONTENTS
# ─────────────────────────────────────────
def print_table_contents(table):
    print("\n── Current DynamoDB Table Contents ──")
    response = table.scan()
    items = sorted(response["Items"], key=lambda x: (x["set_id"], x["device_index"]))

    print(f"{'ENI Name':<25} {'Set':<8} {'ENI ID':<22} {'Private IP':<15} {'Device':<8} {'Status':<12} {'Instance'}")
    print("─" * 110)
    for item in items:
        print(
            f"{item['eni_name']:<25} "
            f"{item['set_id']:<8} "
            f"{item['eni_id']:<22} "
            f"{item['private_ip']:<15} "
            f"{str(item['device_index']):<8} "
            f"{item['status']:<12} "
            f"{item.get('instance_id') or 'None'}"
        )


# ─────────────────────────────────────────
# MAIN — Run setup
# ─────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("  ENI Lifecycle Manager — Setup")
    print("=" * 60)

    # 1. Create DynamoDB table
    table = create_dynamodb_table()

    # 2. Create all ENIs and register in DynamoDB
    create_enis(table)

    # 3. Setup lifecycle hooks on ASG
    setup_lifecycle_hooks()

    # 4. Print table to verify
    print_table_contents(table)

    print("\n Setup complete!")
    print(" Deploy lambda_handler() as your Lambda function.")
