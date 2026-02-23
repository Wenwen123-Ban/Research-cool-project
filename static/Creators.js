      const slides = Array.from(document.querySelectorAll('.creator-slide'));
      const dotsWrap = document.getElementById('dots');
      let currentIndex = 0;

      function getPayload(slide) {
        return {
          slot: slide.dataset.slot,
          name: slide.querySelector('.creator-name').value.trim(),
          role: slide.querySelector('.creator-role').value.trim(),
          description: slide.querySelector('.creator-desc').value.trim()
        };
      }

      function applyProfile(slide, profile) {
        if (!profile) return;
        slide.querySelector('.creator-name').value = profile.name || '';
        slide.querySelector('.creator-role').value = profile.role || slide.querySelector('.creator-role').value;
        slide.querySelector('.creator-desc').value = profile.description || '';
        if (profile.photo) {
          slide.querySelector('.creator-photo').style.backgroundImage = `url(${(window.PROFILE_BASE || "/Profile/") + profile.photo})`;
        }
      }

      async function saveSlide(slide, photoFile = null) {
        const payload = getPayload(slide);
        if (!payload.slot || !payload.name || !payload.role) return;

        const form = new FormData();
        Object.entries(payload).forEach(([k, v]) => form.append(k, v));
        if (photoFile) form.append('photo', photoFile);

        const response = await fetch('/api/creators/upload', { method: 'POST', body: form });
        if (!response.ok) return;

        const result = await response.json();
        if (result?.success && result.profile?.photo) {
          slide.querySelector('.creator-photo').style.backgroundImage = `url(${(window.PROFILE_BASE || "/Profile/") + result.profile.photo})`;
        }
      }

      async function loadProfiles() {
        const response = await fetch('/api/creators/profiles');
        if (!response.ok) return;
        const data = await response.json();
        const profiles = data?.profiles || {};
        slides.forEach((slide) => applyProfile(slide, profiles[slide.dataset.slot]));
      }

      function renderSlides(index) {
        slides.forEach((slide, i) => slide.classList.toggle('active', i === index));
        Array.from(document.querySelectorAll('.dot')).forEach((dot, i) => dot.classList.toggle('active', i === index));
      }

      function shift(step) {
        currentIndex = (currentIndex + step + slides.length) % slides.length;
        renderSlides(currentIndex);
      }

      slides.forEach((slide, i) => {
        const dot = document.createElement('button');
        dot.className = 'dot' + (i === 0 ? ' active' : '');
        dot.addEventListener('click', () => {
          currentIndex = i;
          renderSlides(currentIndex);
        });
        dotsWrap.appendChild(dot);

        const inputFile = slide.querySelector('input[type="file"]');
        slide.querySelector('.creator-photo').addEventListener('click', () => inputFile.click());
        inputFile.addEventListener('change', () => {
          if (inputFile.files && inputFile.files[0]) saveSlide(slide, inputFile.files[0]);
        });

        slide.querySelectorAll('.creator-name, .creator-role, .creator-desc').forEach((field) => {
          field.addEventListener('blur', () => saveSlide(slide));
        });
      });

      document.getElementById('prevBtn').addEventListener('click', () => shift(-1));
      document.getElementById('nextBtn').addEventListener('click', () => shift(1));
      loadProfiles();
    
