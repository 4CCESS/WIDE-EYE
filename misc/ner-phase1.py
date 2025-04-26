import spacy

nlp = spacy.load("en_core_web_lg")
with open('title_entries.txt', 'r') as input_file, open('ner_title_entries.txt', 'w') as output_file:
    for title in input_file:
        title = title.strip()
        if title:  # Skip empty lines
            doc = nlp(title)
            entities = [(ent.text, ent.label_) for ent in doc.ents]
            output_file.write(f"Title: {title}\n")
            output_file.write(f"Entities: {entities}\n\n")

