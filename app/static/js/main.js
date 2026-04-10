/* ================================================================
   main.js  —  Seamless Infinite Innovations
   Handles: navbar dropdowns, hamburger, table search, select-all
   ================================================================ */

document.addEventListener('DOMContentLoaded', () => {

  /* ──────────────────────────────────────────
     1.  DROPDOWN MENUS
  ────────────────────────────────────────── */
  const dropdowns = document.querySelectorAll('.nav-item-dropdown');

  dropdowns.forEach(dropdown => {
    const btn = dropdown.querySelector('.nav-main-btn');
    if (!btn) return;

    btn.addEventListener('click', e => {
      e.stopPropagation();
      const isOpen = dropdown.classList.contains('open');
      // Close all first
      dropdowns.forEach(d => d.classList.remove('open'));
      if (!isOpen) dropdown.classList.add('open');
    });
  });

  // Close on outside click
  document.addEventListener('click', () => {
    dropdowns.forEach(d => d.classList.remove('open'));
  });

  // Prevent closing when clicking inside submenu
  document.querySelectorAll('.nav-submenu').forEach(menu => {
    menu.addEventListener('click', e => e.stopPropagation());
  });


  /* ──────────────────────────────────────────
     2.  HAMBURGER (mobile)
  ────────────────────────────────────────── */
  const hamburger  = document.getElementById('navHamburger');
  const centerLinks = document.getElementById('navCenterLinks');

  if (hamburger && centerLinks) {
    hamburger.addEventListener('click', e => {
      e.stopPropagation();
      centerLinks.classList.toggle('open');
    });
    document.addEventListener('click', e => {
      if (!centerLinks.contains(e.target) && e.target !== hamburger) {
        centerLinks.classList.remove('open');
      }
    });
  }


  /* ──────────────────────────────────────────
     3.  ACTIVE NAV HIGHLIGHT
         Highlights the button whose submenu
         contains a link matching the current URL
  ────────────────────────────────────────── */
  const currentPath = window.location.pathname;

  document.querySelectorAll('.nav-submenu-item').forEach(link => {
    if (link.getAttribute('href') === currentPath) {
      link.classList.add('active');
      const parentBtn = link.closest('.nav-item-dropdown')?.querySelector('.nav-main-btn');
      if (parentBtn) parentBtn.classList.add('active');
    }
  });


  /* ──────────────────────────────────────────
     4.  LIVE TABLE SEARCH
         Works on any input with class .tbl-search-input
         inside a .table-card
  ────────────────────────────────────────── */
  document.querySelectorAll('.tbl-search-input').forEach(input => {
    input.addEventListener('input', () => {
      const q = input.value.toLowerCase().trim();
      const card = input.closest('.table-card');
      if (!card) return;
      card.querySelectorAll('table.seamless-table tbody tr').forEach(row => {
        row.style.display = row.textContent.toLowerCase().includes(q) ? '' : 'none';
      });
    });
  });


  /* ──────────────────────────────────────────
     5.  SELECT-ALL CHECKBOX
  ────────────────────────────────────────── */
  const selectAll = document.getElementById('selectAllCb');
  if (selectAll) {
    selectAll.addEventListener('change', () => {
      document.querySelectorAll('input[name="select_news"]').forEach(cb => {
        cb.checked = selectAll.checked;
      });
    });
  }


  /* ──────────────────────────────────────────
     6.  PAGINATION (UI-only; backend handles real paging)
  ────────────────────────────────────────── */
  document.querySelectorAll('.pagination').forEach(pg => {
    pg.querySelectorAll('.pg-btn').forEach(btn => {
      btn.addEventListener('click', () => {
        // Only mark numeric buttons active (not arrows)
        if (btn.dataset.page) {
          pg.querySelectorAll('.pg-btn').forEach(b => b.classList.remove('active'));
          btn.classList.add('active');
        }
      });
    });
  });

});
