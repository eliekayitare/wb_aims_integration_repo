// Function to show a toast message
function showToast(message, type = "info") {
  const toastContainer = document.getElementById("toast-container");
  if (!toastContainer) return; // Exit if no container is found

  // Create the toast element
  const toast = document.createElement("div");
  toast.classList.add("toast", type); // Add 'info', 'error', etc. based on the type
  toast.innerText = message;

  // Append the toast to the container
  toastContainer.appendChild(toast);

  // Automatically remove the toast after 3 seconds
  setTimeout(() => {
    toast.remove();
  }, 3000); // Remove the toast after 3 seconds
}
