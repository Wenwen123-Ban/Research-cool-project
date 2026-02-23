    let currentRole = 'student', 
        masterBooks = [], 
        masterUsers = [],
        masterAdmins = [],
        masterTransactions = [],
        pendingRequests = [],
        adminHistory = JSON.parse(localStorage.getItem('adminHistory') || '[]'), 
        isStaff = false, 
        activeFilterCat = 'All',
        categoryToDelete = null,
        staffSessionID = localStorage.getItem('adminSchoolId') || '',
        staffSessionToken = localStorage.getItem('adminToken') || '';

    function getAuthHeaders() {
        const token = staffSessionToken || localStorage.getItem('adminToken') || '';
        return token ? { Authorization: token } : {};
    }

    const editModal = new bootstrap.Modal(document.getElementById('editModal'));
    const leaderboardProfileModal = new bootstrap.Modal(document.getElementById('leaderboardProfileModal'));
    const transactionDetailModal = new bootstrap.Modal(document.getElementById('transactionDetailModal'));
    const borrowConfirmModal = new bootstrap.Modal(document.getElementById('borrowConfirmModal'));
    let pendingBorrowBookNo = '';
    let pendingBorrowSchoolId = '';

    window.onload = () => {
        mountAdminDropdown();
        if(localStorage.getItem('isStaffAuth') === 'true') {
            executeUnlock(localStorage.getItem('adminName'), localStorage.getItem('adminPhoto'), localStorage.getItem('adminSchoolId'), localStorage.getItem('adminToken'));
        }
        loadData(true);
        setInterval(() => { document.getElementById('liveClock').innerText = new Date().toLocaleTimeString(); }, 1000);
        setInterval(() => {
            loadData(false);
        }, 10000);
    };

    async function loadData(resetFilter = false) {
        try {
            const preservedFilterCat = activeFilterCat;
            const authHeaders = getAuthHeaders();
            const [bRes, uRes, aRes, tRes] = await Promise.all([
                fetch('/api/books', { headers: authHeaders }),
                fetch('/api/users', { headers: authHeaders }), 
                fetch('/api/admins', { headers: authHeaders }),
                fetch('/api/transactions', { headers: authHeaders })
            ]);

            if ([bRes, uRes, aRes, tRes].some(res => !res.ok)) {
                throw new Error('Unauthorized or failed API request');
            }
            
            masterBooks = await bRes.json();
            const allUsers = await uRes.json();
            masterAdmins = await aRes.json();
            masterTransactions = await tRes.json();

            if (!Array.isArray(masterBooks)) masterBooks = [];
            if (!Array.isArray(masterAdmins)) masterAdmins = [];
            if (!Array.isArray(masterTransactions)) masterTransactions = [];
            const normalizedUsers = Array.isArray(allUsers) ? allUsers : [];
            
            masterUsers = normalizedUsers.filter(u => u.status !== 'pending');
            pendingRequests = normalizedUsers.filter(u => u.status === 'pending');

            document.getElementById('syncDot').classList.add('sync-online');
            document.getElementById('systemStateText').innerText = "Live & Synced";
            
            updatePendingBadge();
            activeFilterCat = resetFilter ? 'All' : preservedFilterCat;
            renderCategoryPills();
            filterInventory(); // Re-apply active category/search filters to fresh data
            syncMonitor();
            renderUsersList();
            renderRequestsList();
            renderAdminHistory();
            
        } catch(e) { 
            console.error("Data Sync Failed", e); 
            document.getElementById('syncDot').classList.remove('sync-online');
            document.getElementById('systemStateText').innerText = "Connection Lost";
        }
    }

    // --- DYNAMIC CATEGORIES (NEW FEATURE) ---
    function renderCategoryPills() {
        // Extract unique categories from loaded books
        const uniqueCats = [...new Set(masterBooks.map(b => b.category))].sort();
        // Add default ones if missing to keep UI consistent
        const defaults = ['General', 'Mathematics', 'Science', 'Literature'];
        defaults.forEach(d => { if(!uniqueCats.includes(d)) uniqueCats.push(d); });
        
        const container = document.getElementById('categoryPillContainer');
        // Preserve "All" pill
        let html = `<div class="cat-pill ${activeFilterCat==='All'?'active':''}" onclick="setCategoryFilter('All', this)">All Collections</div>`;
        
        uniqueCats.forEach(cat => {
            const safeCat = String(cat).replace(/'/g, "\\'");
            html += `<div class="cat-pill ${activeFilterCat===cat?'active':''}" onclick="setCategoryFilter('${safeCat}', this)">${cat}</div>`;
        });
        container.innerHTML = html;
        
        // Also update the dropdowns (Bulk & Edit)
        updateDropdowns(uniqueCats);
    }

    function updateDropdowns(categories) {
        const bulkSel = document.getElementById('batchCategorySelect');
        const editSel = document.getElementById('editCategory');

        const currentBulk = bulkSel.value;
        bulkSel.innerHTML = categories.map(c => `<option value="${c}">Target: ${c}</option>`).join('');
        bulkSel.value = categories.includes(currentBulk) ? currentBulk : (categories[0] || 'General');

        const currentEdit = editSel.value;
        editSel.innerHTML = categories.map(c => `<option value="${c}">${c}</option>`).join('');
        editSel.value = categories.includes(currentEdit) ? currentEdit : (categories[0] || 'General');
    }

    function addCustomCategory() {
        const newCat = prompt("Enter Name for New Category:");
        if(newCat && newCat.trim() !== "") {
            const clean = newCat.trim();
            // Add to dropdown immediately for use
            const bulkSel = document.getElementById('batchCategorySelect');
            const opt = document.createElement("option");
            opt.value = clean;
            opt.text = "Target: " + clean;
            bulkSel.add(opt);
            bulkSel.value = clean; // Select it automatically
            alert(`Category "${clean}" created. You can now import books into it.`);
        }
    }

    function confirmDeleteCategoryFromDropdown(){
        const select = document.getElementById('batchCategorySelect');
        const selectedCategory = select.value;

        if(!selectedCategory || selectedCategory === 'All'){
            alert('This category cannot be deleted.');
            return;
        }

        categoryToDelete = selectedCategory;
        const modal = new bootstrap.Modal(
            document.getElementById('deleteCategoryModal')
        );
        modal.show();
    }

    async function executeCategoryDelete(){
        if(!categoryToDelete) return;

        const res = await fetch('/api/delete_category',{
            method:'POST',
            headers:{'Content-Type':'application/json'},
            body:JSON.stringify({category:categoryToDelete})
        });

        const data = await res.json();

        if(data.success){
            activeFilterCat = 'All';
            loadData(true);
        } else {
            alert('Delete failed.');
        }

        const modal = bootstrap.Modal.getInstance(document.getElementById('deleteCategoryModal'));
        if (modal) modal.hide();
        categoryToDelete = null;
    }

    // --- BULK REGISTER (FIXED) ---
    async function submitBulk() {
        const text = document.getElementById('bulkArea').value;
        if(!text.trim()) return alert("Please enter book data.");

        const res = await fetch('/api/bulk_register', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                text: text,
                category: document.getElementById('batchCategorySelect').value,
                clear_first: document.getElementById('wipeCheck').checked
            })
        });
        const data = await res.json();
        
        if(data.success) {
            // Handle both legacy and new backend keys
            const count = data.items_added || data.added || 0;
            addHistory(`Bulk Import: ${count} books added`);
            alert(`Success! ${count} books registered.`);
            loadData(); // Force refresh
            document.getElementById('bulkArea').value = ''; // Clear input
        } else {
            alert("Error: " + (data.message || "Import failed. Check console."));
        }
    }

    // --- STANDARD UI LOGIC (RETAINED) ---
    function updatePendingBadge() {
        const badge = document.getElementById('pendingBadge');
        if(pendingRequests.length > 0) {
            badge.innerText = pendingRequests.length;
            badge.style.display = 'inline-block';
            badge.classList.add('animate-pulse');
        } else { badge.style.display = 'none'; }
    }

    function switchView(view) {
        document.querySelectorAll('.view-section').forEach(s => s.classList.remove('active'));
        document.querySelectorAll('.nav-link').forEach(l => l.classList.remove('active', 'text-white'));
        document.querySelectorAll('.nav-link').forEach(l => l.classList.add('text-white-50'));

        const linkMap = {
            'console': { id: 'linkConsole', title: 'Command Dashboard' },
            'requests': { id: 'linkRequests', title: 'Registration Queue' },
            'users': { id: 'linkUsers', title: 'User Directory' },
            'inventory': { id: 'linkInventory', title: 'Inventory Manager' },
            'leaderboard': { id: 'linkLeaderboard', title: 'Monthly Leaderboards' }
        };

        const target = linkMap[view];
        if(view === 'leaderboard' && !isStaff) {
            alert('Security Lock Active');
            return;
        }
        if(target) {
            document.getElementById(view + 'View').classList.add('active');
            const link = document.getElementById(target.id);
            link.classList.add('active', 'text-white');
            link.classList.remove('text-white-50');
            document.getElementById('viewTitle').innerText = target.title;
        }
        if(view === 'requests') renderRequestsList();
        if(view === 'leaderboard') loadAdminLeaderboards();
    }

    function renderRequestsList() {
        const tbody = document.getElementById('requestsBody');
        tbody.innerHTML = pendingRequests.map(u => `
            <tr>
                <td class="ps-4"><img src="${window.PROFILE_BASE || '/Profile/'}${u.photo || 'default.png'}" class="user-row-img shadow-sm"></td>
                <td><code class="fw-bold text-dark">${u.school_id}</code></td>
                <td class="fw-bold">${u.name}</td>
                <td><span class="badge badge-pending">PENDING</span></td>
                <td class="text-end pe-4">
                    <button class="btn btn-sm btn-success me-2 fw-bold px-3" onclick="decideRequest('${u.school_id}', 'approve')">Approve</button>
                    <button class="btn btn-sm btn-outline-danger fw-bold px-3" onclick="decideRequest('${u.school_id}', 'reject')">Reject</button>
                </td>
            </tr>
        `).join('') || '<tr><td colspan="5" class="text-center py-5 text-muted fw-bold">No pending registrations.</td></tr>';
    }

    async function decideRequest(id, action) {
        if(!isStaff) return alert("Security Lock Active");
        if(!confirm(`Confirm ${action.toUpperCase()}?`)) return;
        // Note: Ensure /api/approve_user exists in backend, otherwise this will fail silently
        const endpoint = action === 'approve' ? '/api/approve_user' : '/api/reject_user';
        try {
            const res = await fetch(endpoint, { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ school_id: id }) });
            if((await res.json()).success) { loadData(); }
        } catch(e) {}
    }

    function renderUsersList() {
        const query = document.getElementById('userSearch').value.toLowerCase();
        const typeFilter = document.getElementById('userTypeFilter').value;
        const tbody = document.getElementById('usersListBody');
        let combined = [...masterUsers.map(u => ({...u, type: 'student'})), ...masterAdmins.map(a => ({...a, type: 'admin'}))];
        const filtered = combined.filter(u => (u.name.toLowerCase().includes(query) || u.school_id.includes(query)) && (typeFilter === 'all' || u.type === typeFilter));

        tbody.innerHTML = filtered.map(u => `
            <tr>
                <td class="ps-4"><img src="${window.PROFILE_BASE || '/Profile/'}${u.photo || 'default.png'}" class="user-row-img shadow-sm"></td>
                <td><code class="fw-bold text-dark">${u.school_id}</code></td>
                <td class="fw-bold">${u.name}</td>
                <td><span class="badge ${u.type === 'admin' ? 'bg-danger' : 'bg-primary'}">${u.type.toUpperCase()}</span></td>
                <td><span class="status-pill badge-available">Active</span></td>
                <td class="text-end pe-4">${isStaff ? `<button class="btn btn-sm btn-light border" onclick="deleteRecord('user', '${u.school_id}', '${u.type}')"><i class="fas fa-trash text-danger"></i></button>` : '<i class="fas fa-lock text-muted"></i>'}</td>
            </tr>`).join('') || '<tr><td colspan="6" class="text-center py-5 text-muted">No records found.</td></tr>';
    }

    function renderInventory(data) {
        const tbody = document.getElementById('inventoryBody');
        tbody.innerHTML = data.map(b => `
            <tr>
                <td width="150" class="ps-4"><code class="inventory-code">${b.book_no}</code></td>
                <td><div class="inventory-title">${b.title}</div><div class="small text-muted text-uppercase fw-bold" style="font-size:0.65rem">${b.category}</div></td>
                <td><span class="status-pill badge-${b.status.toLowerCase()}">${b.status}</span></td>
                <td class="text-end pe-4">${isStaff ? `<button class="btn btn-sm btn-light border me-1 inventory-action" onclick="openEdit('book', '${b.book_no}', '${b.title}', '${b.category}')"><i class="fas fa-pen"></i></button> <button class="btn btn-sm btn-light border inventory-action" onclick="deleteRecord('book', '${b.book_no}')"><i class="fas fa-trash"></i></button>` : ''}</td>
            </tr>`).join('');
    }

    function openEdit(type, id, name, extra) {
        document.getElementById('editType').value = type;
        document.getElementById('editID').value = id;
        document.getElementById('editName').value = name;
        const bookFields = document.getElementById('bookOnlyFields');
        if(type === 'book') {
            bookFields.style.display = 'block';
            document.getElementById('editCategory').value = extra;
        } else {
            bookFields.style.display = 'none';
            document.getElementById('editID').dataset.role = extra;
        }
        editModal.show();
    }

    async function saveEdits() {
        const type = document.getElementById('editType').value;
        const id = document.getElementById('editID').value;
        const name = document.getElementById('editName').value;
        let endpoint = type === 'book' ? '/api/update_book' : '/api/update_member';
        let payload = type === 'book' ? { book_no: id, title: name, category: document.getElementById('editCategory').value } : { school_id: id, name: name, type: document.getElementById('editID').dataset.role };
        const res = await fetch(endpoint, { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload) });
        if((await res.json()).success) { editModal.hide(); addHistory(`Updated ${type}: ${id}`); loadData(); }
    }

    async function deleteRecord(type, id, role = '') {
        if(!confirm(`Delete ${type} ${id}?`)) return;
        let endpoint = type === 'book' ? '/api/delete_book' : '/api/delete_member';
        let payload = type === 'book' ? { book_no: id } : { school_id: id, type: role };
        const res = await fetch(endpoint, { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload) });
        if((await res.json()).success) { addHistory(`Deleted ${type}: ${id}`); loadData(); }
    }

    async function attemptLogin() {
        const u = document.getElementById('loginUser').value;
        const p = document.getElementById('loginPass').value;
        try {
            const res = await fetch('/api/login', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ school_id: u, password: p }) });
            const data = await res.json();
            if(data.success && data.profile.is_staff) {
                localStorage.setItem('isStaffAuth', 'true');
                localStorage.setItem('adminName', data.profile.name);
                localStorage.setItem('adminPhoto', data.profile.photo);
                localStorage.setItem('adminSchoolId', data.profile.school_id || u);
                localStorage.setItem('adminToken', data.token || '');
                executeUnlock(data.profile.name, data.profile.photo, data.profile.school_id || u, data.token || '');
            } else { showLoginError(); }
        } catch (e) { showLoginError(); }
    }

    function executeUnlock(name, photo, schoolId = '', token = '') {
        isStaff = true;
        staffSessionID = (schoolId || localStorage.getItem('adminSchoolId') || '').toLowerCase();
        staffSessionToken = token || localStorage.getItem('adminToken') || '';
        document.getElementById('mainBody').classList.add('is-unlocked');
        document.getElementById('loginForm').style.display = 'none';
        document.getElementById('adminProfile').style.display = 'block';
        document.getElementById('activeAdminName').innerText = name;
        document.getElementById('headerAvatar').src = `${window.PROFILE_BASE || '/Profile/'}${photo}`;
        document.getElementById('activeAdminPhoto').src = `${window.PROFILE_BASE || '/Profile/'}${photo}`;
        document.getElementById('authStatusBadge').className = "alert alert-success py-2 small fw-bold text-center border-0 shadow-sm rounded-4";
        document.getElementById('authStatusBadge').innerHTML = '<i class="fas fa-check-circle me-2"></i>AUTHORIZED';
        const link = document.getElementById('linkLeaderboard');
        if (link) link.style.display = 'block';
        addHistory(`System Unlocked by: ${name}`);
        loadData(true);
    }

    async function submitUser() {
        if(!isStaff) return alert("System Locked");
        const fd = new FormData();
        fd.append('name', document.getElementById('regName').value);
        fd.append('school_id', document.getElementById('regID').value);
        fd.append('password', document.getElementById('regPass').value);
        const photo = document.getElementById('regPhoto').files[0];
        if(photo) fd.append('photo', photo);
        const endpoint = (currentRole === 'admin') ? '/api/register_librarian' : '/api/register_student';
        const res = await fetch(endpoint, { method: 'POST', body: fd });
        if((await res.json()).success) { addHistory(`Manual Register: ${document.getElementById('regID').value}`); alert("User Added"); loadData(); }
    }

    function findMemberById(schoolId) {
        const sid = String(schoolId || '').toLowerCase();
        return [...masterUsers, ...masterAdmins].find(u => String(u.school_id || '').toLowerCase() === sid) || null;
    }

    function showTransactionInfo(bookNo) {
        const transaction = masterTransactions.find(t => t.book_no === bookNo && (t.status === 'Borrowed' || t.status === 'Reserved'));
        if (!transaction) return;
        const member = findMemberById(transaction.school_id) || {};
        document.getElementById('transactionModalTitle').innerText = 'Reservation Info';
        document.getElementById('transactionModalBody').innerHTML = `
            <div class="d-flex align-items-center gap-3 mb-3">
                <img src="${window.PROFILE_BASE || '/Profile/'}${member.photo || 'default.png'}" class="rounded-circle" style="width:58px;height:58px;object-fit:cover;" alt="profile">
                <div>
                    <div class="fw-bold text-dark">${member.name || transaction.borrower_name || transaction.school_id}</div>
                    <div class="small text-muted">ID: ${transaction.school_id || '-'}</div>
                </div>
            </div>
            <div class="border rounded-3 p-3 bg-light">
                <div><span class="fw-bold text-dark">Book No:</span> <code>${transaction.book_no || '-'}</code></div>
                <div class="mt-1"><span class="fw-bold text-dark">Title:</span> ${transaction.title || 'Unknown Title'}</div>
                <div class="mt-1"><span class="fw-bold text-dark">Pickup Date:</span> ${transaction.pickup_date || '-'}</div>
                <div class="mt-1"><span class="fw-bold text-dark">Reserved At:</span> ${transaction.reserved_at || transaction.date || '-'}</div>
            </div>`;
        transactionDetailModal.show();
    }

    function showBorrowedInfo(bookNo) {
        const transaction = masterTransactions.find(t => t.book_no === bookNo && t.status === 'Borrowed');
        if (!transaction) {
            alert('Borrowed details are available once the reservation is converted to Borrowed.');
            return;
        }
        document.getElementById('transactionModalTitle').innerText = 'Borrowed Schedule';
        document.getElementById('transactionModalBody').innerHTML = `
            <div class="border rounded-3 p-3 bg-light">
                <div><span class="fw-bold text-dark">Book:</span> <code>${transaction.book_no || '-'}</code> - ${transaction.title || 'Unknown Title'}</div>
                <div class="mt-2"><span class="fw-bold text-dark">Borrowed Date:</span> ${transaction.borrow_date || transaction.date || '-'}</div>
                <div class="mt-1"><span class="fw-bold text-dark">Return Due Date:</span> ${transaction.return_date || '-'}</div>
                <div class="mt-1"><span class="fw-bold text-dark">Reserved At:</span> ${transaction.reserved_at || transaction.date || '-'}</div>
                            </div>`;
        transactionDetailModal.show();
    }

    async function syncMonitor() {
        const active = masterTransactions.filter(t => ['Borrowed', 'Reserved', 'Unreturned'].includes(t.status));
        document.getElementById('monitorBody').innerHTML = active.map(t => {
            const infoButton = `<button class="btn btn-sm btn-light border me-1" onclick="showTransactionInfo('${t.book_no}')">Info</button>`;
            const borrowedButton = `<button class="btn btn-sm btn-primary me-1" onclick="openBorrowModal('${t.book_no}','${t.school_id}')" ${t.status === 'Reserved' && isStaff ? '' : 'disabled'}>Borrow</button>`;
            const releaseButton = `<button class="btn btn-sm btn-outline-danger" onclick="releaseAsset('${t.book_no}')" ${isStaff ? '' : 'disabled'}>Release</button>`;
            const dateLabel = t.status === 'Reserved' ? (t.pickup_date || t.reservation_expiry || '-') : (t.return_date || '-');
            const statusText = t.status === 'Unreturned' ? 'Over Due' : t.status;
            return `<tr>
                <td class="ps-4"><code class="fw-bold text-dark">${t.book_no}</code></td>
                <td class="small fw-bold">${t.title || '-'}</td>
                <td class="small fw-bold">${t.school_id}</td>
                <td>${dateLabel}</td>
                <td><span class="status-pill badge-${t.status.toLowerCase()}">${statusText}</span></td>
                <td class="text-end pe-4">${isStaff ? `${infoButton}${borrowedButton}${releaseButton}` : `<i class="fas fa-lock text-muted"></i>`}</td>
            </tr>`;
        }).join('') || '<tr><td colspan="6" class="text-center py-4 text-muted">No active transactions.</td></tr>';
    }

    function openBorrowModal(bookNo, schoolId) {
        const tx = masterTransactions.find(t => t.book_no === bookNo && t.school_id === schoolId && t.status === 'Reserved');
        if (!tx) return alert('Reservation not found.');
        pendingBorrowBookNo = bookNo;
        pendingBorrowSchoolId = schoolId;
        document.getElementById('borrowModalBookTitle').value = tx.title || bookNo;
        document.getElementById('borrowModalBookNumber').value = bookNo;
        document.getElementById('borrowModalBorrowerName').value = tx.borrower_name || schoolId;
        document.getElementById('borrowModalUser').value = schoolId;
        document.getElementById('borrowModalPickupDate').value = tx.pickup_date || tx.reservation_expiry || '-';
        document.getElementById('borrowModalReturnDate').value = '';
        borrowConfirmModal.show();
    }

    async function confirmBorrowAction() {
        if (!pendingBorrowBookNo || !pendingBorrowSchoolId) return;
        const returnDate = document.getElementById('borrowModalReturnDate').value;
        if (!returnDate) return alert('Please select a return date.');
        const res = await fetch('/api/process_transaction', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
            body: JSON.stringify({
                book_no: pendingBorrowBookNo,
                school_id: pendingBorrowSchoolId,
                action: 'borrow',
                return_date: returnDate
            })
        });
        const data = await res.json();
        if (!res.ok || !data.success) {
            alert(data.message || 'Unable to process borrow.');
            return;
        }
        borrowConfirmModal.hide();
        addHistory(`Borrow confirmed: ${pendingBorrowBookNo} for ${pendingBorrowSchoolId}`);
        pendingBorrowBookNo = '';
        pendingBorrowSchoolId = '';
        loadData();
    }


    // --- NEW: Leaderboard API rendering (independent from inventory refresh) ---
    async function loadAdminLeaderboards() {
        if (!isStaff) return;
        try {
            const leaderboardRes = await fetch('/api/monthly_leaderboard');
            const leaderboard = await leaderboardRes.json();
            const borrowers = leaderboard.top_borrowers || [];
            const books = leaderboard.top_books || [];

            document.getElementById('topBorrowersBody').innerHTML = borrowers.map((r, i) => `
                <tr role="button" onclick="openLeaderboardProfile('${r.school_id}')">
                    <td class="ps-4 fw-bold">#${r.rank || i + 1}</td>
                    <td>
                        <div class="d-flex align-items-center gap-2">
                            <img src="${window.PROFILE_BASE || '/Profile/'}${r.photo || 'default.png'}" class="rounded-circle" style="width:36px;height:36px;object-fit:cover;" alt="${r.name}">
                            <div>
                                <div class="fw-bold">${r.name || r.school_id}</div>
                                <div class="small text-muted">${r.school_id}</div>
                            </div>
                        </div>
                    </td>
                    <td>${r.total_borrowed}</td>
                </tr>
            `).join('') || '<tr><td colspan="3" class="text-center text-muted py-4">No borrower data this month.</td></tr>';

            document.getElementById('topBooksBody').innerHTML = books.length > 0
                ? books.map((r, i) => `<tr><td class="ps-4 fw-bold">#${r.rank || i + 1}</td><td><code>${r.book_no}</code></td><td>${r.total_borrowed}</td></tr>`).join('')
                : '<tr><td colspan="3" class="text-center text-muted py-4">No book data this month.</td></tr>';
        } catch (e) {
            document.getElementById('topBorrowersBody').innerHTML = '<tr><td colspan="3" class="text-center text-danger py-4">Failed to load borrowers leaderboard.</td></tr>';
            document.getElementById('topBooksBody').innerHTML = '<tr><td colspan="3" class="text-center text-danger py-4">Failed to load books leaderboard.</td></tr>';
        }
    }

    async function openLeaderboardProfile(id) {
        try {
            const res = await fetch('/api/leaderboard_profile/' + encodeURIComponent(id));
            const data = await res.json();
            if (!res.ok || !data.success) throw new Error(data.message || 'Unable to load profile.');

            const p = data.profile;
            document.getElementById('leaderboardProfilePhoto').src = `${window.PROFILE_BASE || '/Profile/'}${p.photo || 'default.png'}`;
            document.getElementById('leaderboardProfileName').innerText = p.name || p.school_id;
            document.getElementById('leaderboardProfileId').innerText = `ID: ${p.school_id || '-'}`;
            document.getElementById('leaderboardProfileTotal').innerText = p.total_borrowed ?? 0;
            document.getElementById('leaderboardProfileBook').innerText = p.most_borrowed_book || 'No records';
            leaderboardProfileModal.show();
        } catch (e) {
            alert('Failed to load leaderboard profile.');
        }
    }

    async function releaseAsset(b_no) {
        if(!confirm("Return " + b_no + "?")) return;
        const res = await fetch('/api/process_transaction', { method: 'POST', headers: {'Content-Type': 'application/json', ...getAuthHeaders()}, body: JSON.stringify({ book_no: b_no, action: 'return' }) });
        if((await res.json()).success) { addHistory(`Returned Book: ${b_no}`); loadData(); }
    }

    function updateTimers() {
        document.querySelectorAll('.timer').forEach(el => {
            if(!el.dataset.expiry){ el.innerText = 'Awaiting pickup'; return; }
            const diff = new Date(el.dataset.expiry) - new Date();
            el.innerText = diff <= 0 ? "OVERDUE" : `${Math.floor(diff / 60000)}m ${Math.floor((diff % 60000) / 1000)}s`;
            if(diff <= 0) el.classList.add('text-danger', 'fw-black');
        });
    }

    function showLoginError() { document.getElementById('loginError').style.display = 'block'; setTimeout(() => { document.getElementById('loginError').style.display = 'none'; }, 3000); }

    function mountAdminDropdown() {
        const globalDropdownContainer = document.getElementById('global-dropdown-container');
        const adminFloatCard = document.getElementById('adminFloatCard');
        if (globalDropdownContainer && adminFloatCard && adminFloatCard.parentElement !== globalDropdownContainer) {
            globalDropdownContainer.appendChild(adminFloatCard);
        }

        document.addEventListener('click', (event) => {
            const card = document.getElementById('adminFloatCard');
            const trigger = document.getElementById('adminProfileTrigger');
            if (!card || !trigger || !card.classList.contains('active')) return;
            if (!card.contains(event.target) && !trigger.contains(event.target)) {
                card.classList.remove('active');
            }
        });

        window.addEventListener('resize', updateAdminDropdownPosition);
        window.addEventListener('scroll', updateAdminDropdownPosition, true);
    }

    function updateAdminDropdownPosition() {
        const trigger = document.getElementById('adminProfileTrigger');
        const card = document.getElementById('adminFloatCard');
        if (!trigger || !card) return;

        const triggerRect = trigger.getBoundingClientRect();
        const dropdownWidth = card.offsetWidth || 380;
        const spacing = 12;
        const maxLeft = window.innerWidth - dropdownWidth - 12;
        const left = Math.min(Math.max(12, triggerRect.right - dropdownWidth), maxLeft);

        card.style.top = `${triggerRect.bottom + spacing}px`;
        card.style.left = `${left}px`;
    }

    function toggleAdminCard(event) {
        if (event) event.stopPropagation();
        const card = document.getElementById('adminFloatCard');
        if (!card) return;
        card.classList.toggle('active');
        if (card.classList.contains('active')) updateAdminDropdownPosition();
    }

    function setRole(role) { currentRole = role; document.getElementById('btnStudent').classList.toggle('active', role === 'student'); document.getElementById('btnAdmin').classList.toggle('active', role === 'admin'); }
    async function logout() {
        try {
            await fetch('/api/logout', { method: 'POST', headers: getAuthHeaders() });
        } catch (e) {}
        isStaff = false;
        staffSessionID = '';
        staffSessionToken = '';
        localStorage.removeItem('isStaffAuth');
        localStorage.removeItem('adminName');
        localStorage.removeItem('adminPhoto');
        localStorage.removeItem('adminSchoolId');
        localStorage.removeItem('adminToken');
        document.getElementById('mainBody').classList.remove('is-unlocked');
        document.getElementById('loginForm').style.display = 'block';
        document.getElementById('adminProfile').style.display = 'none';
        const link = document.getElementById('linkLeaderboard');
        if (link) link.style.display = 'none';
        loadData();
    }
    function addHistory(msg) { adminHistory.unshift({ msg, time: new Date().toLocaleTimeString() }); localStorage.setItem('adminHistory', JSON.stringify(adminHistory.slice(0, 20))); renderAdminHistory(); }
    function renderAdminHistory() { const log = document.getElementById('adminActionLog'); if(log) log.innerHTML = adminHistory.map(h => `<div class="history-item"><div class="fw-bold">${h.msg}</div><div class="text-muted smaller">${h.time}</div></div>`).join(''); }
    function clearHistory() { adminHistory = []; localStorage.removeItem('adminHistory'); renderAdminHistory(); }
    function setCategoryFilter(cat, el) {
        activeFilterCat = cat;
        document.querySelectorAll('.cat-pill')
            .forEach(p => p.classList.remove('active'));
        el.classList.add('active');
        filterInventory();
    }
    function filterInventory() {
        const q = document.getElementById('inventorySearch').value.toLowerCase();
        const filtered = masterBooks.filter(b =>
            (b.title.toLowerCase().includes(q) ||
             b.book_no.toLowerCase().includes(q)) &&
            (activeFilterCat === 'All' || b.category === activeFilterCat)
        );
        renderInventory(filtered);
    }
