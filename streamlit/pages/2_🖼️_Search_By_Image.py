import streamlit as st
from PIL import Image

st.title("🖼️ Search By Image")

st.write("Upload an image and the app will search for similar images.")

uploaded_file = st.file_uploader("Choose an image...", type=["jpg", "jpeg", "png"])

if uploaded_file is not None:
    image = Image.open(uploaded_file)
    st.image(image, caption="Uploaded Image.", use_container_width=True)
    st.write("Searching for similar images...")

    # Display a gallery of 5 similar images (using stock images from the internet)
    similar_images = [
        "https://loremflickr.com/500/500",
        "https://picsum.photos/500/500",
        "https://baconmockup.com/500/500",
        "https://placebear.com/500/500",
        "https://picsum.photos/500/500",
    ]

    st.write("Similar images:")
    cols = st.columns(5)
    for col, img_url in zip(cols, similar_images):
        col.image(img_url, use_container_width=True)
