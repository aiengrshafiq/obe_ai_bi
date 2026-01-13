async def process_user_query(user_text: str):
    # Step 1: Get DDL Context
    context = get_table_metadata() 
    
    # Step 2: Call PAI-EAS (Qwen) to get the JSON Plan
    # We force the LLM to return the 'DataQueryPlan' structure defined above
    query_plan_json = await llm_client.generate_plan(user_text, context)
    
    # Step 3: Parse into Pydantic Model (Validation happens here!)
    plan = DataQueryPlan(**query_plan_json)
    
    if plan.intent == 'general_chat':
        return {"type": "text", "content": plan.reasoning}
        
    # Step 4: Convert Plan -> Safe SQL
    sql_query = sql_builder.compile(plan)
    
    # Step 5: Return to UI for Confirmation (Human-in-the-Loop)
    return {
        "type": "review_required",
        "plan": plan.dict(),
        "generated_sql": sql_query
    }