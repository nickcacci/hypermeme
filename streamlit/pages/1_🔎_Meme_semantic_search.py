import streamlit as st

st.title("🔎 Meme Semantic Search")

st.write("Use the form below to search for a meme using different criteria:")

with st.form(key="meme_search_form"):
    visual_description = st.text_input("Visual Description")
    image_text = st.text_input("Image Text")
    explainer = st.text_input("Explainer")
    everything = st.text_input("Search Everything")

    submit_button = st.form_submit_button(label="Search")

if submit_button:
    st.write("Searching for memes with the following criteria:")
    st.write(f"Visual Description: {visual_description}")
    st.write(f"Image Text: {image_text}")
    st.write(f"Explainer: {explainer}")
    st.write(f"Search Everything: {everything}")
