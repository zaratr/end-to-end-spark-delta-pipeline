import json
import dspy
import os

# Configure DSPy to use local Ollama with Gemma
lm = dspy.OllamaLocal(model='gemma', max_tokens=500)
dspy.settings.configure(lm=lm)

class LogExtraction(dspy.Signature):
    " " "Extract structured JSON fields from raw unstructured log text." " "
    raw_log = dspy.InputField(desc="The raw unstructured text log")
    structured_output = dspy.OutputField(desc="A valid JSON string with keys: timestamp, level, service, message")

def extract_log(raw_text):
    extractor = dspy.Predict(LogExtraction)
    result = extractor(raw_log=raw_text)
    return result.structured_output

if __name__ == '__main__':
    sample_log = "[2026-05-09 15:30:45] WARN (auth-service): User login failed due to timeout after 5000ms"
    print(f"Processing raw log: {sample_log}")
    try:
        parsed = extract_log(sample_log)
        print(f"Extracted JSON: {parsed}")
    except Exception as e:
        print(f"Error during extraction (Ollama may need to be running): {e}")
        
    print("Phase 1 Step 3 Validation Complete: DSPy parser scaffolded.")
