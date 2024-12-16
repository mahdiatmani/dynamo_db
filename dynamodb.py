import streamlit as st
import db_connection
from botocore.exceptions import ClientError
import pandas as pd
import json

# Initialize DynamoDB table
table_name = 'Users'
table = db_connection.DBconnection().Table(table_name)

def log_query(operation, params):
    """Log the query details"""
    st.sidebar.write(f"### Last {operation} Query")
    st.sidebar.json(params)

def add_user(user_id, first_name, email):
    """Add a new user to DynamoDB"""
    try:
        # Prepare query parameters
        query_params = {
            "TableName": table_name,
            "Item": {
                "UserID": {"N": str(user_id)},
                "FirstName": {"S": first_name},
                "Email": {"S": email}
            }
        }
        
        # Log the query parameters
        log_query("PutItem", query_params)
        
        # Execute the put item operation
        table.put_item(
            Item={
                'UserID': int(user_id),
                'FirstName': first_name,
                'Email': email
            }
        )
        st.success(f"User {user_id} added successfully!")
        return True
    except ClientError as e:
        st.error(f"Error adding user: {e}")
        return False

def view_users():
    """Retrieve all users from DynamoDB"""
    try:
        # Prepare scan query parameters
        query_params = {
            "TableName": table_name,
            "Select": "ALL_ATTRIBUTES"
        }
        
        # Log the query parameters
        log_query("Scan", query_params)
        
        # Execute the scan operation
        response = table.scan()
        return response.get('Items', [])
    except ClientError as e:
        st.error(f"Error retrieving users: {e}")
        return []

def update_user(user_id, new_first_name, new_email):
    """Update an existing user in DynamoDB"""
    try:
        # Prepare update query parameters
        query_params = {
            "TableName": table_name,
            "Key": {
                "UserID": {"N": str(user_id)}
            },
            "UpdateExpression": "SET FirstName = :first_name, Email = :email",
            "ExpressionAttributeValues": {
                ":first_name": {"S": new_first_name},
                ":email": {"S": new_email}
            }
        }
        
        # Log the query parameters
        log_query("UpdateItem", query_params)
        
        # Execute the update operation
        table.update_item(
            Key={'UserID': int(user_id)},
            UpdateExpression="SET FirstName = :first_name, Email = :email",
            ExpressionAttributeValues={
                ':first_name': new_first_name, 
                ':email': new_email
            }
        )
        st.success(f"User {user_id} updated successfully!")
        return True
    except ClientError as e:
        st.error(f"Error updating user: {e}")
        return False

def delete_user(user_id):
    """Delete a user from DynamoDB"""
    try:
        # Prepare delete query parameters
        query_params = {
            "TableName": table_name,
            "Key": {
                "UserID": {"N": str(user_id)}
            }
        }
        
        # Log the query parameters
        log_query("DeleteItem", query_params)
        
        # Execute the delete operation
        table.delete_item(Key={'UserID': int(user_id)})
        st.success(f"User {user_id} deleted successfully!")
        return True
    except ClientError as e:
        st.error(f"Error deleting user: {e}")
        return False

def main():
    """Streamlit main application"""
    st.title("DynamoDB User Management System")
    
    # Sidebar for navigation and query logging
    menu = st.sidebar.selectbox(
        "Menu",
        ["Add User", "View Users", "Update User", "Delete User"]
    )
    
    # Add User Section
    if menu == "Add User":
        st.header("Add New User")
        col1, col2 = st.columns(2)
        
        with col1:
            user_id = st.text_input("User ID")
        with col2:
            first_name = st.text_input("First Name")
        
        email = st.text_input("Email")
        
        if st.button("Add User"):
            if user_id and first_name and email:
                add_user(user_id, first_name, email)
            else:
                st.warning("Please fill in all fields")
    
    # View Users Section
    elif menu == "View Users":
        st.header("All Users")
        users = view_users()
        
        if users:
            df = pd.DataFrame(users)
            df = df[['UserID', 'FirstName', 'Email']]
            st.dataframe(df)
        else:
            st.info("No users found")
    
    # Update User Section
    elif menu == "Update User":
        st.header("Update User")
        col1, col2 = st.columns(2)
        
        with col1:
            user_id = st.text_input("User ID to Update")
        with col2:
            new_first_name = st.text_input("New First Name")
        
        new_email = st.text_input("New Email")
        
        if st.button("Update User"):
            if user_id and new_first_name and new_email:
                update_user(user_id, new_first_name, new_email)
            else:
                st.warning("Please fill in all fields")
    
    # Delete User Section
    elif menu == "Delete User":
        st.header("Delete User")
        user_id = st.text_input("User ID to Delete")
        
        if st.button("Delete User"):
            if user_id:
                delete_user(user_id)
            else:
                st.warning("Please enter a User ID")

# Run the Streamlit app
if __name__ == "__main__":
    main()