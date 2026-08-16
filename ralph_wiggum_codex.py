#!/usr/bin/env python3
# Ralph Wiggum Codex CLI Prompt Generator

import subprocess
import timeb

def generate_ralph_quote(prompt):
    """Generate a Ralph Wiggum quote using Codex CLI"""
    command = f"codex generate --prompt \"{prompt}\" --length 100"
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    return result.stdout

if __name__ == "__main__":
    # Ralph's overnight iterative loop
    start_time = time.time()
    loop_count = 0
    accumulated_prompt = """
    Complete the Proboards scraper to scrape https://oatcakefanzine.proboards.com/
    The scraper should:
    1. Navigate through the forum structure
    2. Extract thread titles, posts, authors, dates
    3. Handle pagination
    4. Save data in a structured format (JSON/CSV)
    5. Be robust with error handling and rate limiting
    
    Focus on making it work for the Oatcake fanzine forum specifically."
    
    # Run loop for 8 hours (overnight)
    while time.time() - start_time < 28800:
        loop_count += 1
        print(f"D'oh! Iteration {loop_count}: Starting Ralph's quote generator... (I'll be back in the morning))
        
        # Run the prompt
        quote = generate_ralph_quote(accumulated_prompt)
        
        # Save to file
        with open(f"ralph_quotes_{loop_count}.txt", "w") as f:
            f.write(quote)
        
        # Feed output back into prompt for next iteration
        accumulated_prompt += f"\n\n{quote}"
        
        # Add Ralph's signature pause
        time.sleep(2)  # D'oh! Thinking time
    
    print("D'oh! All iterations complete! (I think...)")
    print("I'll be back
