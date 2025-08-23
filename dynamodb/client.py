import boto3
from boto3.dynamodb.conditions import Key, Attr
from flask import Flask, request, jsonify
from flask_cors import CORS
import hashlib
import uuid
from datetime import datetime
import json
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# DynamoDB configuration
AWS_REGION = 'us-east-1'  # Change as needed
TABLE_NAME = 'hackathon_users'

# Initialize DynamoDB
dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
dynamodb_client = boto3.client('dynamodb', region_name=AWS_REGION)

class DynamoDBManager:
    def __init__(self, table_name):
        self.table_name = table_name
        self.table = None
        self._initialize_table()
    
    def _initialize_table(self):
        """Initialize table connection"""
        try:
            self.table = dynamodb.Table(self.table_name)
            # Test table exists
            self.table.load()
            logger.info(f"Connected to existing table: {self.table_name}")
        except Exception as e:
            logger.warning(f"Table doesn't exist: {e}")
            self.table = None
    
    def create_table(self):
        """Create the DynamoDB table with optimal configuration"""
        try:
            # Check if table already exists
            existing_tables = dynamodb_client.list_tables()['TableNames']
            if self.table_name in existing_tables:
                logger.info(f"Table {self.table_name} already exists")
                self.table = dynamodb.Table(self.table_name)
                return True
            
            # Create new table
            table = dynamodb.create_table(
                TableName=self.table_name,
                KeySchema=[
                    {
                        'AttributeName': 'user_id',
                        'KeyType': 'HASH'  # Partition key
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'user_id',
                        'AttributeType': 'S'
                    },
                    {
                        'AttributeName': 'email',
                        'AttributeType': 'S'
                    }
                ],
                GlobalSecondaryIndexes=[
                    {
                        'IndexName': 'email-index',
                        'KeySchema': [
                            {
                                'AttributeName': 'email',
                                'KeyType': 'HASH'
                            }
                        ],
                        'Projection': {
                            'ProjectionType': 'ALL'
                        },
                        'BillingMode': 'PAY_PER_REQUEST'
                    }
                ],
                BillingMode='PAY_PER_REQUEST'
            )
            
            # Wait for table to be created
            table.wait_until_exists()
            self.table = table
            logger.info(f"Table {self.table_name} created successfully!")
            return True
            
        except Exception as e:
            logger.error(f"Error creating table: {e}")
            return False
    
    def hash_password(self, password):
        """Hash password using SHA256"""
        return hashlib.sha256(password.encode('utf-8')).hexdigest()
    
    def insert_user(self, username, email, password, additional_data=None):
        """Insert new user into DynamoDB"""
        try:
            if not self.table:
                raise Exception("Table not initialized")
            
            # Check if email already exists
            if self.get_user_by_email(email):
                return None, "Email already exists"
            
            user_id = str(uuid.uuid4())
            timestamp = datetime.utcnow().isoformat()
            
            user_item = {
                'user_id': user_id,
                'username': username,
                'email': email.lower(),  # Store email in lowercase
                'password_hash': self.hash_password(password),
                'created_at': timestamp,
                'updated_at': timestamp,
                'verified': False,
                'active': True,
                'login_count': 0,
                'last_login': None,
                'profile_data': additional_data or {}
            }
            
            # Insert user
            response = self.table.put_item(
                Item=user_item,
                ConditionExpression=Attr('email').not_exists()  # Ensure email uniqueness
            )
            
            logger.info(f"User created: {user_id}")
            
            # Return user without password hash
            user_item.pop('password_hash')
            return user_item, None
            
        except dynamodb_client.exceptions.ConditionalCheckFailedException:
            return None, "Email already exists"
        except Exception as e:
            logger.error(f"Error inserting user: {e}")
            return None, str(e)
    
    def get_user_by_id(self, user_id):
        """Retrieve user by user_id"""
        try:
            if not self.table:
                raise Exception("Table not initialized")
            
            response = self.table.get_item(
                Key={'user_id': user_id}
            )
            
            if 'Item' in response:
                user = response['Item']
                user.pop('password_hash', None)  # Remove password hash
                return user
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting user by ID: {e}")
            return None
    
    def get_user_by_email(self, email):
        """Retrieve user by email using GSI"""
        try:
            if not self.table:
                raise Exception("Table not initialized")
            
            response = self.table.query(
                IndexName='email-index',
                KeyConditionExpression=Key('email').eq(email.lower())
            )
            
            if response['Items']:
                return response['Items'][0]
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting user by email: {e}")
            return None
    
    def authenticate_user(self, email, password):
        """Authenticate user and update login stats"""
        try:
            user = self.get_user_by_email(email)
            
            if not user:
                return None, "User not found"
            
            if not user.get('active', True):
                return None, "Account deactivated"
            
            # Check password
            if user['password_hash'] != self.hash_password(password):
                return None, "Invalid password"
            
            # Update login statistics
            self.update_user_login_stats(user['user_id'])
            
            # Remove password hash and return user
            user.pop('password_hash', None)
            return user, None
            
        except Exception as e:
            logger.error(f"Authentication error: {e}")
            return None, str(e)
    
    def update_user_login_stats(self, user_id):
        """Update user login statistics"""
        try:
            current_time = datetime.utcnow().isoformat()
            
            self.table.update_item(
                Key={'user_id': user_id},
                UpdateExpression="SET last_login = :time, login_count = login_count + :inc, updated_at = :time",
                ExpressionAttributeValues={
                    ':time': current_time,
                    ':inc': 1
                }
            )
            
        except Exception as e:
            logger.error(f"Error updating login stats: {e}")
    
    def update_user(self, user_id, update_data):
        """Update user data"""
        try:
            if not self.table:
                raise Exception("Table not initialized")
            
            # Don't allow updating sensitive fields
            forbidden_fields = ['user_id', 'password_hash', 'created_at']
            update_data = {k: v for k, v in update_data.items() if k not in forbidden_fields}
            
            if not update_data:
                return None, "No valid fields to update"
            
            # Add updated timestamp
            update_data['updated_at'] = datetime.utcnow().isoformat()
            
            # Build update expression
            update_expression = "SET "
            expression_attribute_values = {}
            expression_attribute_names = {}
            
            for key, value in update_data.items():
                # Handle reserved keywords
                attr_name = f"#{key}"
                attr_value = f":{key}"
                
                update_expression += f"{attr_name} = {attr_value}, "
                expression_attribute_names[attr_name] = key
                expression_attribute_values[attr_value] = value
            
            update_expression = update_expression.rstrip(', ')
            
            response = self.table.update_item(
                Key={'user_id': user_id},
                UpdateExpression=update_expression,
                ExpressionAttributeNames=expression_attribute_names,
                ExpressionAttributeValues=expression_attribute_values,
                ReturnValues="ALL_NEW",
                ConditionExpression=Attr('user_id').exists()
            )
            
            user = response['Attributes']
            user.pop('password_hash', None)
            return user, None
            
        except dynamodb_client.exceptions.ConditionalCheckFailedException:
            return None, "User not found"
        except Exception as e:
            logger.error(f"Error updating user: {e}")
            return None, str(e)
    
    def delete_user(self, user_id):
        """Soft delete user (mark as inactive)"""
        try:
            user, error = self.update_user(user_id, {'active': False})
            if error:
                return False, error
            
            logger.info(f"User soft-deleted: {user_id}")
            return True, None
            
        except Exception as e:
            logger.error(f"Error deleting user: {e}")
            return False, str(e)
    
    def get_all_users(self, active_only=True):
        """Get all users (use with caution for large datasets)"""
        try:
            if not self.table:
                raise Exception("Table not initialized")
            
            scan_kwargs = {}
            if active_only:
                scan_kwargs['FilterExpression'] = Attr('active').eq(True)
            
            response = self.table.scan(**scan_kwargs)
            users = response.get('Items', [])
            
            # Remove password hashes
            for user in users:
                user.pop('password_hash', None)
            
            return users, None
            
        except Exception as e:
            logger.error(f"Error getting all users: {e}")
            return None, str(e)
    
    def get_table_stats(self):
        """Get table statistics"""
        try:
            if not self.table:
                return None, "Table not initialized"
            
            response = dynamodb_client.describe_table(TableName=self.table_name)
            table_info = response['Table']
            
            stats = {
                'table_name': table_info['TableName'],
                'table_status': table_info['TableStatus'],
                'item_count': table_info.get('ItemCount', 'Unknown'),
                'table_size_bytes': table_info.get('TableSizeBytes', 'Unknown'),
                'billing_mode': table_info.get('BillingModeSummary', {}).get('BillingMode', 'Unknown'),
                'creation_date': table_info.get('CreationDateTime', 'Unknown')
            }
            
            return stats, None
            
        except Exception as e:
            logger.error(f"Error getting table stats: {e}")
            return None, str(e)

# Initialize database manager
db_manager = DynamoDBManager(TABLE_NAME)

# API Routes
@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "table_connected": db_manager.table is not None
    })

@app.route('/setup', methods=['POST'])
def setup_database():
    """Initialize database table"""
    success = db_manager.create_table()
    if success:
        return jsonify({"message": "Database setup complete", "table_name": TABLE_NAME})
    else:
        return jsonify({"error": "Failed to setup database"}), 500

@app.route('/stats', methods=['GET'])
def get_stats():
    """Get database statistics"""
    stats, error = db_manager.get_table_stats()
    if error:
        return jsonify({"error": error}), 500
    return jsonify({"stats": stats})

@app.route('/users', methods=['POST'])
def create_user():
    """Create new user"""
    try:
        data = request.json
        username = data.get('username')
        email = data.get('email')
        password = data.get('password')
        additional_data = data.get('profile_data', {})
        
        if not all([username, email, password]):
            return jsonify({"error": "Missing required fields: username, email, password"}), 400
        
        user, error = db_manager.insert_user(username, email, password, additional_data)
        if error:
            return jsonify({"error": error}), 400
        
        return jsonify({"user": user, "message": "User created successfully"}), 201
        
    except Exception as e:
        logger.error(f"Error in create_user: {e}")
        return jsonify({"error": "Internal server error"}), 500

@app.route('/users/authenticate', methods=['POST'])
def authenticate():
    """Authenticate user"""
    try:
        data = request.json
        email = data.get('email')
        password = data.get('password')
        
        if not all([email, password]):
            return jsonify({"error": "Email and password required"}), 400
        
        user, error = db_manager.authenticate_user(email, password)
        if error:
            return jsonify({"error": error}), 401
        
        return jsonify({"user": user, "message": "Authentication successful"})
        
    except Exception as e:
        logger.error(f"Error in authenticate: {e}")
        return jsonify({"error": "Internal server error"}), 500

@app.route('/users/<user_id>', methods=['GET'])
def get_user(user_id):
    """Get user by ID"""
    try:
        user = db_manager.get_user_by_id(user_id)
        if user:
            return jsonify({"user": user})
        else:
            return jsonify({"error": "User not found"}), 404
            
    except Exception as e:
        logger.error(f"Error in get_user: {e}")
        return jsonify({"error": "Internal server error"}), 500

@app.route('/users/<user_id>', methods=['PUT'])
def update_user(user_id):
    """Update user"""
    try:
        data = request.json
        user, error = db_manager.update_user(user_id, data)
        if error:
            return jsonify({"error": error}), 400
        
        return jsonify({"user": user, "message": "User updated successfully"})
        
    except Exception as e:
        logger.error(f"Error in update_user: {e}")
        return jsonify({"error": "Internal server error"}), 500

@app.route('/users/<user_id>', methods=['DELETE'])
def delete_user(user_id):
    """Delete (deactivate) user"""
    try:
        success, error = db_manager.delete_user(user_id)
        if error:
            return jsonify({"error": error}), 400
        
        return jsonify({"message": "User deactivated successfully"})
        
    except Exception as e:
        logger.error(f"Error in delete_user: {e}")
        return jsonify({"error": "Internal server error"}), 500

@app.route('/users', methods=['GET'])
def list_users():
    """List all active users"""
    try:
        users, error = db_manager.get_all_users()
        if error:
            return jsonify({"error": error}), 500
        
        return jsonify({"users": users, "count": len(users)})
        
    except Exception as e:
        logger.error(f"Error in list_users: {e}")
        return jsonify({"error": "Internal server error"}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)