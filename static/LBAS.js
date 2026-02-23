      let currentID = null;
      let currentToken = null;
      let selectedCategory = "All";
      let selectedStars = 5;
      let availableCategories = [];
      let leaderboardProfileModal = null;
      let dataInterval = null;
      let timerInterval = null;
      const ALL_COLLECTION_LIMIT = 20;
      const CATEGORY_LIMIT = 10;
      const MAX_ACTIVE_RESERVATIONS = 5;
      const BORROW_DURATION_MS = 2 * 24 * 60 * 60 * 1000;
      let userReservations = {};
      let userActiveLeases = {};
      let pendingReservationRequests = new Set();
      let pendingReserveBookNo = null;
      let latestBooksByCode = {};
      let allCollectionOrder = [];
      let categoryCollectionOrder = {};

      function shuffleBooks(books) {
        const shuffled = [...books];
        for (let i = shuffled.length - 1; i > 0; i--) {
          const j = Math.floor(Math.random() * (i + 1));
          [shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];
        }
        return shuffled;
      }

      function getRandomizedAllCollection(books, limit) {
        const grouped = books.reduce((acc, book) => {
          const key = book.category || "General";
          if (!acc[key]) acc[key] = [];
          acc[key].push(book);
          return acc;
        }, {});

        const pools = Object.values(grouped)
          .map((group) => shuffleBooks(group))
          .filter((group) => group.length > 0);

        const mixed = [];
        while (mixed.length < limit && pools.some((pool) => pool.length > 0)) {
          pools.forEach((pool) => {
            if (pool.length > 0 && mixed.length < limit) {
              mixed.push(pool.pop());
            }
          });
        }
        return shuffleBooks(mixed);
      }

      function normalizeReservation(transaction) {
        return {
          book_no: transaction.book_no,
          expiry: transaction.expiry || null,
        };
      }

      function getReservationKey(schoolID) {
        return String(schoolID || "").trim().toLowerCase();
      }

      function getNormalizedStatus(transaction) {
        return String(transaction?.status || "").trim().toLowerCase();
      }

      function getTransactionSchoolId(transaction) {
        return getReservationKey(transaction?.school_id ?? transaction?.schoolID ?? transaction?.user_id);
      }

      function parseTransactionPayload(payload) {
        if (Array.isArray(payload)) return payload;
        if (payload && Array.isArray(payload.transactions)) return payload.transactions;
        return [];
      }

      function normalizeLease(transaction) {
        let expiryDate = null;
        if (transaction.expiry) {
          expiryDate = new Date(transaction.expiry);
        } else if (transaction.date && getNormalizedStatus(transaction) === "borrowed") {
          expiryDate = new Date(new Date(transaction.date).getTime() + BORROW_DURATION_MS);
        }

        const book = latestBooksByCode[transaction.book_no] || {};
        return {
          book_no: transaction.book_no,
          title: transaction.title || book.title || "Unknown Title",
          status: transaction.status,
          expiry: expiryDate ? expiryDate.toISOString() : null,
          pickup_date: transaction.pickup_date || transaction.reservation_expiry || transaction.expiry || "",
          return_date: transaction.return_date || "",
        };
      }

      function cleanupExpiredReservationsForUser(schoolID) {
        const key = getReservationKey(schoolID);
        const now = Date.now();

        if (!Array.isArray(userReservations[key])) {
          userReservations[key] = [];
        }

        userReservations[key] = userReservations[key].filter((reservation) => {
          const expiryTime = new Date(reservation.expiry).getTime();
          return Number.isFinite(expiryTime) && expiryTime > now;
        });

        return userReservations[key];
      }

      function syncUserReservations(transactions, schoolID) {
        if (!schoolID) return [];

        const key = getReservationKey(schoolID);

        const reservations = transactions
          .filter(
            (transaction) =>
              getTransactionSchoolId(transaction) === key &&
              getNormalizedStatus(transaction) === "reserved",
          )
          .map(normalizeReservation);

        userReservations[key] = reservations;
        return cleanupExpiredReservationsForUser(schoolID);
      }

      function cleanupExpiredLeasesForUser(schoolID) {
        const key = getReservationKey(schoolID);
        const now = Date.now();

        if (!Array.isArray(userActiveLeases[key])) {
          userActiveLeases[key] = [];
        }

        userActiveLeases[key] = userActiveLeases[key].filter((lease) => {
          if (getNormalizedStatus(lease) === "reserved") return true;
          const expiryTime = new Date(lease.expiry).getTime();
          return Number.isFinite(expiryTime) && expiryTime > now;
        });

        return userActiveLeases[key];
      }

      function syncUserActiveLeases(transactions, schoolID) {
        if (!schoolID) return [];
        const key = getReservationKey(schoolID);
        const leases = transactions
          .filter(
            (transaction) => {
              const status = getNormalizedStatus(transaction);
              return (
                getTransactionSchoolId(transaction) === key &&
                (status === "reserved" || status === "borrowed")
              );
            },
          )
          .map(normalizeLease);

        userActiveLeases[key] = leases;
        return cleanupExpiredLeasesForUser(schoolID);
      }

      function renderActiveLeases() {
        const key = getReservationKey(currentID);
        const active = cleanupExpiredLeasesForUser(key)
          .slice()
          .sort((a, b) => new Date(a.expiry) - new Date(b.expiry));
        const reservationCount = active.filter(
          (lease) => getNormalizedStatus(lease) === "reserved",
        ).length;

        const reservationCountNode = document.getElementById("reservationCount");
        if (reservationCountNode) {
          reservationCountNode.textContent = String(reservationCount);
        }

        const activeLeaseLabel = document.getElementById("activeLeaseLabel");
        if (activeLeaseLabel) {
          activeLeaseLabel.innerHTML = `<i class="fas fa-history me-1"></i> Active Leases (${active.length}/${MAX_ACTIVE_RESERVATIONS})`;
        }

        document.getElementById("activeHistory").innerHTML =
          active
            .map(
              (lease) => `
                <div class="p-3 border rounded-3 mb-2 bg-light d-flex justify-content-between align-items-center gap-2">
                  <div>
                    <div class="fw-bold text-dark">${lease.title}</div>
                    <code class="small text-primary fw-bold">${lease.book_no}</code><br>
                    <span class="badge ${getNormalizedStatus(lease) === "reserved" ? "bg-warning text-dark" : "bg-success"}">${lease.status}</span>
                  </div>
                  <div class="text-end">
                    ${getNormalizedStatus(lease) === "reserved" ? `<span class="small fw-bold text-warning d-block">Pick up on: ${lease.pickup_date || "-"}</span>` : `<span class="small fw-bold text-info d-block">Return on: ${lease.return_date || "-"}</span><span class="timer small fw-bold text-primary d-block" data-expiry="${lease.expiry}" data-status="${lease.status}" data-book-no="${lease.book_no}">Calculating...</span>`}
                    ${getNormalizedStatus(lease) === "reserved" ? `<button class="btn btn-sm btn-outline-danger rounded-pill mt-1" onclick="releaseReservation('${lease.book_no}')">Release</button>` : ""}
                  </div>
                </div>`,
            )
            .join("") ||
          '<p class="text-muted small text-center mt-3 border p-3 rounded-3 dashed">No active reservations.</p>';

        updateTimers();
      }

      async function loadReservations() {
        if (!currentID) return;
        try {
          const tRes = await fetch("/api/transactions");
          const trans = parseTransactionPayload(await tRes.json());
          syncUserReservations(trans, currentID);
          syncUserActiveLeases(trans, currentID);
          renderActiveLeases();
        } catch (e) {
          console.error("Unable to refresh reservations.");
        }
      }

      async function fetchUserActiveReservations() {
        await loadReservations();
      }

      async function handleLogin() {
        const id = document.getElementById("school_id_input").value.trim();
        const pwd = document.getElementById("password_input").value.trim();
        if (!id) return;

        const btn = document.getElementById("loginBtn");
        const err = document.getElementById("loginError");
        const errTxt = document.getElementById("errorText");

        btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> VERIFYING...';
        btn.disabled = true;
        err.style.display = "none";

        try {
          const res = await fetch("/api/login", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ school_id: id, password: pwd }),
          });

          const data = await res.json();

          if (data.success) {
            currentID = id;
            currentToken = data.token;
            localStorage.setItem("lbas_id", id);
            localStorage.setItem("lbas_token", currentToken);
            initPortal(data.profile);
          } else {
            err.style.display = "block";
            if (res.status === 401 && data.message.includes("Pending")) {
              showStatusPopup(
                "warning",
                "Approval Pending",
                "Your account is waiting for Librarian review. Please check back later.",
              );
              err.style.display = "none"; // Hide the red error box if showing popup
            } else if (res.status === 404) {
              errTxt.innerText = "ID NOT FOUND / REQUEST REJECTED";
            } else {
              errTxt.innerText = data.message || "AUTHENTICATION FAILED";
            }
          }
        } catch (e) {
          err.style.display = "block";
          errTxt.innerText = "SERVER UNREACHABLE";
        } finally {
          btn.innerText = "SECURE LOGIN";
          btn.disabled = false;
        }
      }

      function previewPhoto(input) {
        if (input.files && input.files[0]) {
          const reader = new FileReader();
          reader.onload = function (e) {
            document.getElementById("previewImg").src = e.target.result;
            document.getElementById("previewImg").style.display = "block";
            document.getElementById("uploadIcon").style.display = "none";
          };
          reader.readAsDataURL(input.files[0]);
        }
      }

      async function submitRegistration() {
        const name = document.getElementById("regName").value;
        const id = document.getElementById("regID").value;
        const pwd = document.getElementById("regPass").value;
        const file = document.getElementById("regPhoto").files[0];

        if (!name || !id || !pwd) return alert("Please fill all fields.");

        const fd = new FormData();
        fd.append("name", name);
        fd.append("school_id", id);
        fd.append("password", pwd);
        if (file) fd.append("photo", file);

        try {
          const res = await fetch("/api/register_student", {
            method: "POST",
            body: fd,
          });
          const data = await res.json();

          toggleModal("registerModal", false); // Close form

          if (data.success) {
            showStatusPopup(
              "success",
              "Application Sent!",
              "Your request has been forwarded to the Librarian. You cannot log in until approved.",
            );
            document.getElementById("regName").value = "";
            document.getElementById("regID").value = "";
            document.getElementById("regPass").value = "";
            document.getElementById("previewImg").src = "";
            document.getElementById("previewImg").style.display = "none";
            document.getElementById("uploadIcon").style.display = "block";
          } else {
            showStatusPopup(
              "error",
              "Registration Failed",
              data.message || "ID already exists or invalid data.",
            );
          }
        } catch (e) {
          alert("Network Error during registration.");
        }
      }

      function showStatusPopup(type, title, msg) {
        const icon = document.getElementById("statusIcon");
        const h4 = document.getElementById("statusTitle");

        document.getElementById("statusMsg").innerText = msg;

        if (type === "success") {
          icon.className = "fas fa-check-circle text-success";
          h4.className = "fw-bold text-success";
        } else if (type === "warning") {
          icon.className = "fas fa-clock text-warning";
          h4.className = "fw-bold text-warning";
        } else {
          icon.className = "fas fa-times-circle text-danger";
          h4.className = "fw-bold text-danger";
        }

        h4.innerText = title;
        toggleModal("statusModal", true);
      }

      function renderCategoryFilters() {
        const list = document.getElementById("catFilterList");
        let html = `<div class="cat-pill ${selectedCategory === "All" ? "active" : ""}" onclick="setCategoryFilter('All')">All Collection</div>`;
        availableCategories.forEach((cat) => {
          html += `<div class="cat-pill ${selectedCategory === cat ? "active" : ""}" onclick="setCategoryFilter('${cat}')">${cat}</div>`;
        });
        list.innerHTML = html;
      }

      async function fetchCategories() {
        try {
          const res = await fetch("/api/categories");
          const cats = await res.json();
          availableCategories = Array.isArray(cats) ? cats : [];
          renderCategoryFilters();
        } catch (e) {
          availableCategories = [
            "General",
            "Mathematics",
            "Science",
            "Literature",
          ];
          renderCategoryFilters();
        }
      }

      function switchPortalView(view) {
        const isLeaderboard = view === "leaderboard";
        document.getElementById("catalogSection").style.display = isLeaderboard
          ? "none"
          : "block";
        document.getElementById("leaderboardSection").style.display =
          isLeaderboard ? "block" : "none";
        document
          .getElementById("catalogMenuBtn")
          .classList.toggle("active", !isLeaderboard);
        document
          .getElementById("leaderboardMenuBtn")
          .classList.toggle("active", isLeaderboard);
        if (isLeaderboard) loadLeaderboard();
      }

      async function loadLeaderboard() {
        try {
          const res = await fetch("/api/monthly_leaderboard");
          const data = await res.json();
          const rows = data.top_borrowers || [];
          document.getElementById("leaderboardBorrowersBody").innerHTML =
            rows
              .map(
                (row, idx) => `
                  <tr role="button" onclick="openLeaderboardProfile('${row.school_id}')">
                    <td class="fw-bold">#${row.rank || idx + 1}</td>
                    <td>
                      <div class="d-flex align-items-center gap-2">
                        <img src="${window.PROFILE_BASE || '/Profile/'}${row.photo || 'default.png'}" class="rounded-circle" style="width:36px;height:36px;object-fit:cover;" alt="${row.name}">
                        <div>
                          <div class="fw-bold">${row.name || row.school_id}</div>
                          <div class="small text-muted">${row.school_id}</div>
                        </div>
                      </div>
                    </td>
                    <td>${row.total_borrowed}</td>
                  </tr>
            `,
              )
              .join("") ||
            '<tr><td colspan="3" class="text-muted text-center py-4">No borrow records yet for this month.</td></tr>';
        } catch (e) {
          document.getElementById("leaderboardBorrowersBody").innerHTML =
            '<tr><td colspan="3" class="text-danger text-center py-4">Unable to load leaderboard.</td></tr>';
        }
      }

      async function openLeaderboardProfile(id) {
        try {
          const res = await fetch(
            "/api/leaderboard_profile/" + encodeURIComponent(id),
          );
          const data = await res.json();
          if (!res.ok || !data.success) throw new Error();
          const p = data.profile;
          document.getElementById("leaderboardProfilePhoto").src =
            `${window.PROFILE_BASE || '/Profile/'}${p.photo || 'default.png'}`;
          document.getElementById("leaderboardProfileName").innerText =
            p.name || p.school_id;
          document.getElementById("leaderboardProfileId").innerText =
            `ID: ${p.school_id || "-"}`;
          document.getElementById("leaderboardProfileTotal").innerText =
            p.total_borrowed ?? 0;
          document.getElementById("leaderboardProfileBook").innerText =
            p.most_borrowed_book || "No records";
          leaderboardProfileModal.show();
        } catch (e) {
          console.error("Unable to load leaderboard profile.");
        }
      }

      function initPortal(profile) {
        if (!profile) return logout();

        if (dataInterval) clearInterval(dataInterval);
        if (timerInterval) clearInterval(timerInterval);

        document.getElementById("loginSection").style.display = "none";
        document.getElementById("portalSection").style.display = "block";

        const isLibrarian = profile.category === "Staff";

        if (isLibrarian) {
          document.getElementById("lbasInfo").style.display = "block";
        }

        document.getElementById("user_type_label").innerText = isLibrarian
          ? "LIBRARIAN MODE"
          : "STUDENT ACCESS";
        document.getElementById("user_type_label").className = isLibrarian
          ? "badge bg-danger text-uppercase mb-1"
          : "badge bg-primary text-uppercase mb-1";
        document.getElementById("database_source").innerText = isLibrarian
          ? "CREDENTIAL: STAFF"
          : "CREDENTIAL: USER";

        document.getElementById("display_name").innerText = profile.name
          ? profile.name.split(" ")[0]
          : "User";
        document.getElementById("full_name").innerText =
          profile.name || "Unknown User";
        document.getElementById("id_val").innerText =
          "ID: " + profile.school_id;
        document.getElementById("user_pic").src = profile.photo
          ? (window.PROFILE_BASE || "/Profile/") + profile.photo
          : `${window.PROFILE_BASE || '/Profile/'}default.png`;
        switchPortalView("catalog");

        fetchCategories();
        fetchUserActiveReservations();
        loadData();
        checkRatingStatus();
        dataInterval = setInterval(loadData, 5000);
        timerInterval = setInterval(updateTimers, 1000);
      }

      async function loadData() {
        if (!currentID) return;
        try {
          const authHeaders = currentToken
            ? { Authorization: currentToken }
            : {};
          const [bRes, tRes] = await Promise.all([
            fetch("/api/books", { headers: authHeaders }),
            fetch("/api/transactions", { headers: authHeaders }),
          ]);

          if (!bRes.ok) {
            document.getElementById("bookContainer").innerHTML =
              '<div class="text-center text-danger mt-5"><i class="fas fa-lock fa-2x mb-3"></i><br>Unable to load books. Please login again.</div>';
            return;
          }

          const books = await bRes.json();
          const trans = parseTransactionPayload(await tRes.json());

          if (!Array.isArray(books)) {
            document.getElementById("bookContainer").innerHTML =
              '<div class="text-center text-danger mt-5"><i class="fas fa-exclamation-triangle fa-2x mb-3"></i><br>Book data is unavailable right now.</div>';
            return;
          }

          latestBooksByCode = books.reduce((acc, book) => {
            acc[book.book_no] = book;
            return acc;
          }, {});
          syncUserReservations(trans, currentID);
          syncUserActiveLeases(trans, currentID);
          const search = document
            .getElementById("searchBar")
            .value.toLowerCase();
          const isSearching = search.length > 0;

          const filtered = books.filter((b) => {
            const matchesSearch =
              b.title.toLowerCase().includes(search) ||
              b.book_no.toLowerCase().includes(search);
            const matchesCategory =
              isSearching ||
              selectedCategory === "All" ||
              b.category === selectedCategory;
            return matchesSearch && matchesCategory;
          });

          if (allCollectionOrder.length === 0) {
            allCollectionOrder = getRandomizedAllCollection(books, ALL_COLLECTION_LIMIT).map(
              (book) => book.book_no,
            );
          }

          const categoryKey = selectedCategory;
          if (
            !isSearching &&
            selectedCategory !== "All" &&
            !categoryCollectionOrder[categoryKey]
          ) {
            categoryCollectionOrder[categoryKey] = shuffleBooks(filtered)
              .slice(0, CATEGORY_LIMIT)
              .map((book) => book.book_no);
          }

          const displayBooks = isSearching
            ? filtered
            : selectedCategory === "All"
              ? allCollectionOrder
                  .map((bookNo) => latestBooksByCode[bookNo])
                  .filter((book) => book && filtered.some((f) => f.book_no === book.book_no))
              : (categoryCollectionOrder[categoryKey] || [])
                  .map((bookNo) => latestBooksByCode[bookNo])
                  .filter(Boolean);

          document.getElementById("bookContainer").innerHTML =
            displayBooks
              .map(
                (b) => `
                <div class="book-item shadow-sm">
                    <span class="status-tag tag-${b.status.toLowerCase()}">${b.status}</span>
                    <span class="text-muted" style="font-size: 10px; font-weight:700;">${b.category.toUpperCase()}</span>
                    <div class="fw-bold text-dark mt-1">${b.title}</div>
                    <code class="small text-primary fw-bold">${b.book_no}</code>
                    ${b.status === "Available" ? `<button class="btn-action shadow-sm" data-book-no="${b.book_no}" onclick="reserveBook('${b.book_no}')">RESERVE BOOK</button>` : ""}
                </div>`,
              )
              .join("") ||
            '<div class="text-center text-muted mt-5"><i class="fas fa-book-open fa-2x mb-3 opacity-25"></i><br>No books found.</div>';

          renderActiveLeases();
        } catch (e) {
          console.error("Data Sync Error");
        }
      }

      function reserveBook(no) {
        const schoolID = String(currentID);
        if (!schoolID) return;
        if (pendingReservationRequests.has(no)) return;

        pendingReserveBookNo = no;
        const selectedBook = latestBooksByCode[no] || {};
        document.getElementById("reserveBorrowerName").value =
          document.getElementById("full_name")?.innerText || "";
        document.getElementById("reserveBorrowerId").value = currentID || "";
        document.getElementById("reserveBookTitle").value = selectedBook.title || "";
        document.getElementById("reserveBookNumber").value = no;
        const pickupDateField = document.getElementById("reservePickupDate");
        const defaultPickupDate = new Date(Date.now() + 24 * 60 * 60 * 1000);
        pickupDateField.value = defaultPickupDate.toISOString().slice(0, 10);
        toggleModal("reserveModal", true);
      }

      async function submitReserveForm() {
        const no = pendingReserveBookNo;
        if (!no) return;

        const reserveButton = document.querySelector(`button[data-book-no="${no}"]`);
        const borrowerName = document
          .getElementById("reserveBorrowerName")
          .value.trim();
        const borrowerId = document
          .getElementById("reserveBorrowerId")
          .value.trim();
        const bookTitle = document
          .getElementById("reserveBookTitle")
          .value.trim();
        const pickupDate = document.getElementById("reservePickupDate").value;

        if (!borrowerName || !borrowerId || !bookTitle || !pickupDate) {
          alert("Please complete reservation details first.");
          return;
        }
        if (pendingReservationRequests.has(no)) return;

        pendingReservationRequests.add(no);
        if (reserveButton) reserveButton.disabled = true;

        try {
          const res = await fetch("/api/reserve", {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              ...(currentToken ? { Authorization: currentToken } : {}),
            },
            body: JSON.stringify({
              book_no: no,
              school_id: currentID,
              borrower_name: borrowerName,
              pickup_date: pickupDate,
              title: bookTitle,
            }),
          });

          const result = await res.json();

          if (!res.ok || !result.success || result.status === "error") {
            alert(result.message || "Unable to complete reservation.");
            return;
          }

          const key = getReservationKey(currentID);
          if (!Array.isArray(userReservations[key])) userReservations[key] = [];
          if (!Array.isArray(userActiveLeases[key])) userActiveLeases[key] = [];

          const leaseTitle = (latestBooksByCode[no] && latestBooksByCode[no].title) ||
            result.title ||
            "Unknown Title";

          userReservations[key].push({
            book_no: no,
            expiry: null,
          });
          userActiveLeases[key] = userActiveLeases[key].filter(
            (lease) => lease.book_no !== no,
          );
          userActiveLeases[key].push({
            book_no: no,
            title: leaseTitle,
            status: "Reserved",
            expiry: null,
          });
          renderActiveLeases();
          toggleModal("reserveModal", false);
          pendingReserveBookNo = null;

          showStatusPopup(
            "success",
            "Reservation Confirmed",
            "Please proceed to the librarian desk before the pickup date.",
          );
          await loadReservations();
          loadData();
        } catch (e) {
          showStatusPopup(
            "error",
            "Action Failed",
            "Unable to complete reservation right now. Please try again.",
          );
        } finally {
          pendingReservationRequests.delete(no);
          if (reserveButton) reserveButton.disabled = false;
        }
      }

      async function releaseReservation(bookNo) {
        if (!currentID || !bookNo) return;
        if (!confirm("Release this reservation?")) return;

        try {
          const res = await fetch("/api/process_transaction", {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              ...(currentToken ? { Authorization: currentToken } : {}),
            },
            body: JSON.stringify({
              book_no: bookNo,
              action: "return",
              school_id: currentID,
            }),
          });
          const data = await res.json();
          if (!res.ok || !data.success) {
            alert(data.message || "Unable to release reservation.");
            return;
          }

          const key = getReservationKey(currentID);
          userReservations[key] = (userReservations[key] || []).filter(
            (reservation) => reservation.book_no !== bookNo,
          );
          userActiveLeases[key] = (userActiveLeases[key] || []).filter(
            (lease) => lease.book_no !== bookNo,
          );
          renderActiveLeases();
          await loadReservations();
          loadData();
        } catch (e) {
          alert("Unable to release reservation right now.");
        }
      }

      function setCategoryFilter(cat) {
        selectedCategory = cat;
        document.querySelectorAll("#catFilterList .cat-pill").forEach((p) => {
          const isAll = cat === "All" && p.innerText.includes("All");
          p.classList.toggle("active", p.innerText.trim() === cat || isAll);
        });
        loadData();
      }

      function updateTimers() {
        let foundExpiredReservation = false;
        document.querySelectorAll(".timer").forEach((el) => {
          if (!el.dataset.expiry) {
            el.innerText = "Awaiting librarian confirmation";
            return;
          }
          const diff = new Date(el.dataset.expiry) - new Date();
          if (diff <= 0) {
            if (el.dataset.status === "Reserved") {
              foundExpiredReservation = true;
              return;
            }
            el.innerText = "EXPIRED";
            el.classList.add("text-danger");
          } else {
            const m = Math.floor(diff / 60000);
            const s = Math.floor((diff % 60000) / 1000);
            el.innerText = `${m}m ${s}s remaining`;
          }
        });

        if (foundExpiredReservation) {
          cleanupExpiredReservationsForUser(currentID);
          cleanupExpiredLeasesForUser(currentID);
          renderActiveLeases();
        }
      }

      async function checkRatingStatus() {
        if (!currentID) return;
        try {
          const res = await fetch(`/api/rating_status/${currentID}`);
          const data = await res.json();
          document.getElementById("fabReview").style.display = data.show
            ? "flex"
            : "none";
        } catch (e) {}
      }

      function toggleModal(id, show) {
        document.getElementById(id).style.display = show ? "flex" : "none";
        if (id === "ratingModal" && show) setStars(5);
      }

      function setStars(count) {
        selectedStars = count;
        document
          .querySelectorAll(".star")
          .forEach((s, i) => s.classList.toggle("selected", i < count));
      }

      async function submitRating() {
        const feedback = document.getElementById("ratingFeedback").value;
        const res = await fetch("/api/rate", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            school_id: currentID,
            token: currentToken,
            stars: selectedStars,
            feedback: feedback,
          }),
        });
        const data = await res.json();
        if (data.success) {
          toggleModal("ratingModal", false);
          checkRatingStatus();
          showStatusPopup(
            "success",
            "Feedback Sent",
            "Thank you for rating LBAS!",
          );
        }
      }

      function toggleAccount() {
        const p = document.getElementById("accountPanel");
        p.style.display = p.style.display === "block" ? "none" : "block";
      }
      async function logout() {
        const token = currentToken;
        if (dataInterval) {
          clearInterval(dataInterval);
          dataInterval = null;
        }
        if (timerInterval) {
          clearInterval(timerInterval);
          timerInterval = null;
        }

        currentID = null;
        currentToken = null;
        userReservations = {};
        userActiveLeases = {};
        pendingReservationRequests = new Set();
        pendingReserveBookNo = null;
        latestBooksByCode = {};
        allCollectionOrder = [];
        categoryCollectionOrder = {};

        localStorage.removeItem("lbas_id");
        localStorage.removeItem("lbas_token");

        document.getElementById("portalSection").style.display = "none";
        document.getElementById("loginSection").style.display = "flex";
        document.getElementById("accountPanel").style.display = "none";
        toggleModal("reserveModal", false);
        document.getElementById("bookContainer").innerHTML = "";

        if (token) {
          try {
            await fetch("/api/logout", { method: "POST", headers: { Authorization: token } });
          } catch (e) {
            console.warn("Logout sync failed", e);
          }
        }
      }

      window.onload = () => {
        leaderboardProfileModal = new bootstrap.Modal(
          document.getElementById("leaderboardProfileModal"),
        );
        fetchCategories();
        const savedID = localStorage.getItem("lbas_id");
        const savedToken = localStorage.getItem("lbas_token");
        if (savedID) {
          currentID = savedID;
          currentToken = savedToken;
          fetch("/api/user/" + savedID)
            .then((r) => r.json())
            .then((d) => {
              if (d.profile && d.profile.status !== "pending")
                initPortal(d.profile);
              else logout();
            })
            .catch(logout);
        }
      };
    
